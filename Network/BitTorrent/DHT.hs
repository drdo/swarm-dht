{- TODO
- Handle SockAddr nicely, this is now always assuming IPv4
- Carefully think about exceptions and what to do about them
- Periodically update the table with nodes close to you
- Perhaps bootstrap should be forked instead of running in newManager
-}

{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE ViewPatterns #-}

module Network.BitTorrent.DHT
  ( -- Re-exports
    ManagerSettings (..)
  , Manager
  , newManager
  , shutdownManager

  , ping
  , findNode
  , getPeers
  , AnnouncePeerResult (..), announcePeer

  , randomNId
  )
where

import Control.Arrow ((&&&))
import Control.Concurrent.Lifted (fork, newEmptyMVar, putMVar, takeMVar)
import Control.Lens (_1, _4, view)
import Control.Monad.Base (MonadBase (..))
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.Maybe (fromJust, isJust)
import Data.Set (Set)
import qualified Data.Vector.Unboxed as VU (replicateM)
import Network.BitTorrent.DHT.Internal
import Network.BitTorrent.DHT.Types
import Network.BitTorrent.Types
import Network.Socket (PortNumber, SockAddr (..))
import Numeric.Natural (Natural)
import System.Random (randomIO)

--------------------------------------------------------------------------------
-- | Ping an address.
ping ∷ MonadBaseControl IO m ⇒ SockAddr → Manager m → m (Maybe Node)
ping addr@(SockAddrInet port address) m = ping_ addr m >>= \case
  Nothing → pure Nothing
  Just (PingResponse nId) → do
    let n = Node nId address port
    fork $ integrateNode n m
    pure $ Just n

-- | Find the k 'Node's in the DHT closest to this 'NodeId'.
findNode ∷ MonadBaseControl IO m ⇒ NodeId → Manager m → m [Node]
findNode targetId m = do
  finalState ← genFind targetId (\n → fireFindNode n targetId m) m
  pure $ cnsResults (managerK $ managerSettings m) . view _4 $ finalState

-- | Find 'Peer's for this 'InfoHash'.
--
-- Additionally returns the k 'Node's closest to this 'InfoHash'
-- and the necessary 'Token's for an 'announcePeer'.
getPeers ∷ MonadBaseControl IO m
  ⇒ InfoHash -- ^ The 'InfoHash' of the torrent to get peers for.
  → Manager m
  → m (Set Peer, [(Node, Token)])
getPeers ihash m = do
  finalState ← genFind ihash (\n → fireGetPeers n ihash m) m
  let ps = view _1 finalState
      ns = map (mnNode &&& (fromJust . snd . mnMark))
         . filter (\(mnMark → (wasQueried, mToken)) → wasQueried && isJust mToken)
         . cnsElems
         . view _4
         $ finalState
  pure (ps, ns)

-- | Result of an 'announcePeer'.
data AnnouncePeerResult
  = Successful -- ^ The announce was successful.
  | UnexpectedNodeId -- ^ The remote node has unexpectedly changed their 'NodeId'.
  | Failed -- ^ The announce failed.
  deriving
    (Eq, Show)

-- | Announces that we are participating in the swarm for this 'InfoHash'.
announcePeer ∷ MonadBaseControl IO m
  ⇒ Node -- ^ The node to announce to.
  → InfoHash -- ^ The 'InfoHash' to announce.
  → (Maybe PortNumber) -- ^ Our port number.
  → Token -- ^ The 'Token' obtained in a previous 'getPeers'.
  → Manager m
  → m AnnouncePeerResult
announcePeer (Node nId address port) ihash mPort token m = do
  resultVar ← newEmptyMVar
  let cb (AnnouncePeerResponse nId') = putMVar resultVar $
        if nId' == nId then Successful else UnexpectedNodeId
      errorCb _ _ = putMVar resultVar Failed
      invalidCb _ _ = putMVar resultVar Failed
      timeoutCb = putMVar resultVar Failed
      addr = SockAddrInet port address
  sendAnnouncePeer addr ihash mPort token m cb errorCb invalidCb timeoutCb
  takeMVar resultVar

--------------------------------------------------------------------------------
-- | Generate a random 'NodeId'
randomNId ∷ MonadBase IO m ⇒ m NodeId
randomNId = NodeId <$> VU.replicateM 160 (liftBase randomIO)
