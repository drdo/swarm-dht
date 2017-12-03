{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NumDecimals #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UnicodeSyntax #-}

module Network.BitTorrent.DHT.Internal
  ( ManagerSettings (..)
  , Callback, ErrorCallback, TimeoutCallback
  , Manager (managerSettings)
  , readManagerTable
  , integrateNode
  , newManager
  , shutdownManager
  , sendQuery
  , InvalidCallback
  , PingResponse (..), PingCallback, sendPing, ping_, pingNode_
  , FindNodeResponse (..), FindNodeCallback, sendFindNode
  , GetPeersResponse (..), GetPeersCallback, sendGetPeers
  , AnnouncePeerResponse (..), AnnouncePeerCallback, sendAnnouncePeer

  , mnNode, mnMark
  , cnsElems, cnsResults
  , fireFindNode, fireGetPeers
  , genFind
  )
where

import Control.Concurrent.Async.Lifted (mapConcurrently)
import Control.Concurrent.Lifted
import Control.Concurrent.STM
import Control.Exception.Lifted (bracketOnError)
import Control.Lens
import Control.Monad (forever, when, void)
import Control.Monad.Base (MonadBase (liftBase))
import Control.Monad.State (StateT, get, modify, execStateT)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Control (MonadBaseControl)
import qualified Crypto.Hash.SHA1 as SHA1 (hash)
import Data.BEncode
import Data.Bool (bool)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS (pack)
import qualified Data.ByteString.Lazy as LBS (fromStrict, toStrict)
import qualified Data.ByteString.UTF8 as BSU8 (fromString)
import Data.Default (Default (def))
import Data.Foldable (traverse_)
import Data.List (sort, sortOn)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, isJust, isNothing)
import Data.Monoid ((<>))
import Data.Set (Set)
import qualified Data.Set as Set (empty, fromList, map, toList)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Void (Void)
import Network.BitTorrent.DHT.KBucket
import Network.BitTorrent.DHT.Types
import Network.BitTorrent.Types
import Network.Socket
import qualified Network.Socket.ByteString as SocketBS (sendTo, recvFrom)
import Numeric.Natural (Natural)
import System.Random (randomIO)
import System.Timeout.Lifted (timeout)

--------------------------------------------------------------------------------
-- Util
--------------------------------------------------------------------------------
toMicroSeconds ∷ NominalDiffTime → Int
toMicroSeconds t = ceiling $ 10^6 * toRational t

randomBS ∷ MonadBase IO m ⇒ Int → m ByteString
randomBS n = BS.pack <$> sequence (replicate n (liftBase randomIO))

randomTId ∷ MonadBase IO m ⇒ m TransactionId
randomTId = TransactionId <$> randomBS 2

mkToken ∷ ByteString → Node → Token
mkToken secret n = Token $ SHA1.hash $ secret <> BSU8.fromString (show n)

--------------------------------------------------------------------------------
-- Manager
--------------------------------------------------------------------------------
data ManagerSettings m = ManagerSettings
  { managerVersion ∷ Maybe ByteString -- ^ Software version to report to other nodes.
                                      -- Default is Nothing.
  , managerAddr ∷ SockAddr -- ^ Address this Manager should bind to.
                           -- Default binds to all addresses at a random port.
  , managerBootstrapNodes ∷ [SockAddr] -- ^ Nodes used for bootstrapping.
                                       -- Default is empty.
  , managerBootstrapTries ∷ Int -- ^ Number of tries until we give up on a bootstrap node.
                                -- Default is 3.
  , managerK ∷ Int -- ^ k parameter as in Kademlia.
                   -- Default is 8.
  , managerAlpha ∷ Int -- ^ α parameter as in Kademlia.
                       -- Default is 3.
  , managerTimeout ∷ Int -- ^ How long to wait until we consider one of our queries timed out.
                         -- Default is 1.5 seconds.
  , managerMaxTries ∷ Int -- ^ Number of tries until we consider a node bad.
                          -- Default is 3.
  , managerGoodTimeout ∷ NominalDiffTime -- ^ How long until a good none is considered questionable.
                                         -- Default is 15 minutes.
  , managerSecretTimeout ∷ NominalDiffTime -- ^ How often to change secret for token generation.
                                           -- Default is 5 minutes.
  , managerLog ∷ String → m () -- ^ This is called with log messages. Needs to be thread-safe.
                               -- Default is const (pure ()).
  , managerGetKnownPeers ∷ InfoHash → m (Set Peer) -- ^ This should return known peers for a given InfoHash.
                                                   -- Default is const (pure empty).
  , managerAddKnownPeer ∷ Peer → m () -- ^ This action is called when someone successfully sends us an announcePeer.
                                      -- Default is const (pure ()).
  }

instance Applicative m ⇒ Default (ManagerSettings m) where
  def = ManagerSettings
    { managerVersion = Nothing
    , managerAddr = SockAddrInet aNY_PORT iNADDR_ANY
    , managerBootstrapNodes = []
    , managerBootstrapTries = 3
    , managerK = 8
    , managerAlpha = 3
    , managerTimeout = 1.5e6 -- 1.5 seconds (in microseconds)
    , managerMaxTries = 3
    , managerGoodTimeout = 15*60 -- 15min (in seconds)
    , managerSecretTimeout = 5*60 -- 5min (in seconds)
    , managerLog = \_ → pure ()
    , managerGetKnownPeers = \_ → pure Set.empty
    , managerAddKnownPeer = \_ → pure ()
    }

-- | Called with the "r" dictionary if our query was successful.
type Callback m = Dict → m ()

-- | Called when the node replied with an error 'Message'.
type ErrorCallback m = ErrorCode → String → m ()

-- | Called if our query timed out.
type TimeoutCallback m = m ()

-- | 'Map' of pending queries.
type PendingMap m = Map (TransactionId, SockAddr)
                        (UTCTime, Callback m, ErrorCallback m, TimeoutCallback m)

-- | Represents our DHT node.
data Manager m = Manager
  { managerSettings ∷ ManagerSettings m -- ^ Settings for this 'Manager'.
  , managerThread ∷ ThreadId -- ^ ThreadId of the main thread.
  , managerSock ∷ Socket
  , managerPendingVar ∷ MVar (PendingMap m) -- ^ Pending queries.
  , managerMyId ∷ NodeId -- ^ Our 'NodeId'.
  , managerTableVar ∷ MVar Table -- ^ Our routing table.
  -- | Our secret for 'Token' generation and when it was last updated.
  , managerSecretVar ∷ MVar (ByteString, UTCTime)
  }

-- | How long to wait for the next incoming message, in microseconds.
getWaitTime ∷ MonadBase IO m ⇒ Manager m → m Int
getWaitTime (Manager { managerSettings = ManagerSettings {..}, .. }) =
  sort . map (view _1) . Map.elems <$> readMVar managerPendingVar >>= \case
    [] → pure managerTimeout
    (time:_) → do
      curTime ← liftBase getCurrentTime
      let δt = toMicroSeconds $ curTime `diffUTCTime` time
      pure $ min δt managerTimeout

-- | Routing table for this 'Manager'.
readManagerTable ∷ MonadBase IO m ⇒ Manager m → m Table
readManagerTable = readMVar . managerTableVar

-- | Current secret for this 'Manager'.
getSecret ∷ MonadBaseControl IO m ⇒ Manager m → m ByteString
getSecret (Manager { managerSettings = ManagerSettings {..}, .. }) =
  modifyMVar managerSecretVar $ \(secret, modified) → do
    t ← liftBase getCurrentTime
    if t `diffUTCTime` modified < managerSecretTimeout
      then pure ((secret, modified), secret)
      else do
        newSecret ← randomBS 20
        newModified ← liftBase getCurrentTime
        managerLog $ "mainThread: secret changed"
        pure ((newSecret, newModified), newSecret)

-- | Make a new 'Manager'.
--
-- This can take some time. Bootstraps before returning the new 'Manager'.
newManager ∷ MonadBaseControl IO m
  ⇒ ManagerSettings m
  → NodeId -- ^ Our 'NodeId'.
  → Maybe Table -- ^ Initial routing table.
  → m (Manager m)
newManager mS@(ManagerSettings {..}) myId mInitTable = do
  bracketOnError
    (liftBase $ socket AF_INET Datagram defaultProtocol)
    (liftBase . close)
    (\sock → do
        managerVar ← newEmptyMVar
        pendingVar ← newMVar Map.empty
        tableVar ← newMVar =<< case mInitTable of
          Nothing → liftBase getCurrentTime >>= \t → pure (KBucket t [])
          Just initTable → pure initTable
        secretVar ← do
          secret ← randomBS 20
          modified ← liftBase getCurrentTime
          newMVar (secret, modified)
        liftBase $ bind sock managerAddr
        mainThread ← forkFinally
          (readMVar managerVar >>= mainLoop)
          (\x → do
              managerLog ("mainThread: died (" ++ show x ++ ")")
              liftBase $ close sock)
        let m = Manager
              { managerSettings = mS
              , managerThread = mainThread
              , managerSock = sock
              , managerPendingVar = pendingVar
              , managerMyId = myId
              , managerTableVar = tableVar
              , managerSecretVar = secretVar
              }
        putMVar managerVar m
        bootstrap m
        pure m)

-- | Shuts down a 'Manager'.
shutdownManager ∷ MonadBase IO m ⇒ Manager m → m ()
shutdownManager = killThread . managerThread

--------------------------------------------------------------------------------
-- Main loop
--------------------------------------------------------------------------------
-- | Handles an incoming 'Query'.
handleQuery ∷ MonadBaseControl IO m
  ⇒ Node
  → MessageHeader
  → Query
  → Manager m
  → m ()
handleQuery n (MessageHeader tId mVersion) query m = do
  let Manager { managerSettings = ManagerSettings {..}, .. } = m
  managerLog $ "mainThread: got query " ++ show query ++ " from " ++ show n
  let replyHeader = MessageHeader tId managerVersion
  reply ← case query of
    Ping → pure $ Response $ Map.fromList [("id", toBEncode managerMyId)]
    FindNode targetId → do
      table ← readMVar managerTableVar
      let ns = map nodeToCompactNode
             . Set.toList
             $ closestNodes managerK targetId table
      pure $ Response $ Map.fromList [("id", toBEncode managerMyId)
                                     ,("nodes", toBEncode ns)]
    GetPeers ihash → do
      token ← getSecret m >>= \secret → pure (mkToken secret n)
      table ← readMVar managerTableVar
      let ns = map nodeToCompactNode
             . Set.toList
             $ closestNodes managerK ihash table
      let d = Map.fromList [("id", toBEncode managerMyId)
                           ,("token", toBEncode token)
                           ,("nodes", toBEncode ns)]
      ps ← map peerToCompactPeer
         . Set.toList
       <$> managerGetKnownPeers ihash
      let d' = if null ps
               then d
               else Map.insert "values" (toBEncode ps) d
      pure $ Response d'
    AnnouncePeer ihash mPort token → do
      secret ← getSecret m
      if token == mkToken secret n
        then do
          managerAddKnownPeer (Peer (nodeAddress n) (maybe (nodePort n) id mPort) Nothing)
          pure $ Response $ Map.fromList [("id", toBEncode managerMyId)]
        else pure $ Error ProtocolError "Invalid token"
  let addr = SockAddrInet (nodePort n) (nodeAddress n)
      msg = Message replyHeader reply
  void $ liftBase $ SocketBS.sendTo managerSock (LBS.toStrict . encode $ msg) addr
  managerLog $ "mainThread: replied to " ++ show n

-- Should probably check that the reply's id matches the expected
-- | Handles an incoming reply.
handleReply ∷ MonadBaseControl IO m
  ⇒ SockAddr
  → MessageHeader
  → Either (ErrorCode, String) Dict
  → Manager m
  → m ()
handleReply addr (MessageHeader tId _) r m = do
  let Manager { managerSettings = ManagerSettings {..}, .. } = m
  managerLog $ "mainThread: response from " ++ show (tId, addr)
  action ← modifyMVar managerPendingVar $ \pending →
    case Map.lookup (tId, addr) pending of
      Nothing →
        let action =
              managerLog $ "mainThread: got reply with invalid tId/addr " ++ show (tId, addr)
        in pure (pending, action)
      Just (_, cb, errorCb, _) →
        let pending' = Map.delete (tId, addr) pending
            action = do
              managerLog $ "mainThread: got reply from " ++ show (tId, addr)
              case r of
                Left (eCode, eStr) → errorCb eCode eStr
                Right args → cb args
        in pure (pending', action)
  action

-- | Handles an incoming 'Message'.
handleMessage ∷ MonadBaseControl IO m
  ⇒ SockAddr
  → Message
  → Manager m
  → m ()
handleMessage addr@(SockAddrInet port address) (Message header body) m = case body of
  Query sourceId query → do
    let n = Node sourceId address port
    integrateNode n m
    handleQuery n header query m
  Response args →
    handleReply addr header (Right args) m
  Error eCode eStr →
    handleReply addr header (Left (eCode, eStr)) m

-- | Cleans up pending queries, timing them out if appropriate.
cleanup ∷ MonadBaseControl IO m ⇒ Manager m → m ()
cleanup m@(Manager { managerSettings = ManagerSettings {..}, ..}) = do
  t ← liftBase getCurrentTime
  cbs ← modifyMVar managerPendingVar $ \pending → do
    let hasTimedOut p = toMicroSeconds (t `diffUTCTime` view _1 p) < managerTimeout
        (pending', timedout) = Map.partition hasTimedOut pending
    pure (pending', map (view _4) . Map.elems $ timedout)
  sequence_ cbs

-- | Main loop for a 'Manager'.
mainLoop ∷ MonadBaseControl IO m ⇒ Manager m → m Void
mainLoop m@(Manager { managerSettings = ManagerSettings {..}, ..}) = forever $ do
  -- Figure out what constant to use (4096)
  getWaitTime m >>= \t → timeout t (liftBase $ SocketBS.recvFrom managerSock 4096) >>= \case
    Nothing → pure ()
    Just (bs, addr) →
      case decode (LBS.fromStrict bs) of
        Nothing → managerLog "mainThread: Received invalid packet"
        Just msg → handleMessage addr msg m
  cleanup m

--------------------------------------------------------------------------------
-- Node Integration
--------------------------------------------------------------------------------
-- | State of a known 'Node'.
data NodeState
  = Good
  | Questionable
  | Bad
  deriving
    (Eq, Show)

-- | Computes the 'NodeState' of a 'Node'.
nodeState
  ∷ Int -- ^ Similar to 'managerMaxTries'.
  → NominalDiffTime -- ^ Similar to 'managerGoodTimeout'.
  → UTCTime -- ^ Current time.
  → (UTCTime, Int) -- ^ Time this node was last seen and how many queries in a row it has failed to reply to.
  → NodeState
nodeState maxTries maxTime time (lastSeen, tries) =
  if | tries >= maxTries → Bad
     | time `diffUTCTime` lastSeen < maxTime → Good
     | otherwise → Questionable

-- | Find a 'Bad' 'Node' in a 'KBucket'.
lookupBadNode ∷ MonadBase IO m
  ⇒ Int -- ^ Similar to 'managerMaxTries'.
  → NominalDiffTime -- ^ Similar to 'managerGoodTimeout'.
  → KBucket
  → m (Maybe Node)
lookupBadNode maxTries maxTime ns = do
  time ← liftBase getCurrentTime
  let badNs = Map.filter ((== Bad) . nodeState maxTries maxTime time) ns
  if Map.null badNs
    then pure Nothing
    else pure $ Just . head . Map.keys $ badNs

-- | Find least recently seen 'Questionable' 'Node' in a 'KBucket'.
lookupLRSQuestionableNode ∷ MonadBase IO m
  ⇒ Int -- ^ Similar to 'managerMaxTries'.
  → NominalDiffTime -- ^ Similar to 'managerGoodTimeout'.
  → KBucket
  → m (Maybe Node)
lookupLRSQuestionableNode maxTries maxTime ns = do
  time ← liftBase getCurrentTime
  let questionableNs = Map.filter ((== Questionable) . nodeState maxTries maxTime time) ns
  if Map.null questionableNs
    then pure Nothing
    else pure $ Just . fst . head . sortOn (view (_2._1))  . Map.toList $ questionableNs

-- | Result for 'tryEvictNode'.
data TryEvictNodeResult
  = Evicted KBucket -- ^ A 'Node' has been evicted.
  | AllGood KBucket -- ^ All 'Node's in this 'KBucket' are 'Good'.
  deriving
    (Eq, Show)

-- | Try to evict a 'Node' from a 'KBucket'.
tryEvictNode ∷ MonadBaseControl IO m
  ⇒ KBucket
  → Manager m
  → m TryEvictNodeResult
tryEvictNode ns m@(Manager { managerSettings = ManagerSettings{..} }) =
  lookupBadNode managerMaxTries managerGoodTimeout ns >>= \case
    Nothing → go ns
    Just n → pure $ Evicted (Map.delete n ns)
  where
    go ns =
      lookupLRSQuestionableNode managerMaxTries managerGoodTimeout ns >>= \case
        Nothing → pure $ AllGood ns
        Just n → try0 ns n
    evict ns n = pure $ Evicted (Map.delete n ns)
    try0 ns n = pingNode_ n m >>= \case
      Nothing → try1 ns n
      Just (PingResponse nId) | nId == nodeId n → markGood ns n
                              | otherwise → evict ns n
    try1 ns n = pingNode_ n m >>= \case
      Nothing → evict ns n
      Just (PingResponse nId) | nId == nodeId n → markGood ns n
                              | otherwise → evict ns n
    markGood ns n = do
      time ← liftBase getCurrentTime
      go (Map.insert n (time, 0) ns)

-- | Integrate a 'Node' into this 'Manager'\'s routing table.
integrateNode ∷ MonadBaseControl IO m ⇒ Node → Manager m → m ()
integrateNode n m@(Manager { managerSettings = ManagerSettings{..}, ..}) =
  modifyMVar_ managerTableVar $ \table →
    case dryInsertNode n managerMyId managerK table of
      Successful (KBucket lastChanged ns, ctx) → do
        time ← liftBase getCurrentTime
        pure $ fst $ root (KBucket time (Map.insert n (time, 0) ns), ctx)
      AlreadyPresent (KBucket lastChanged ns, ctx) → do
        time ← liftBase getCurrentTime
        pure $ fst $ root (KBucket time (Map.insert n (time, 0) ns), ctx) -- We are assuming the node just replied, so we set the tries at 0
      Full (KBucket lastChanged ns, ctx) →
        tryEvictNode ns m >>= \case
          Evicted ns' → do
            time ← liftBase getCurrentTime
            pure $ fst $ root (KBucket time (Map.insert n (time, 0) ns'), ctx)
          AllGood ns' → do
            time ← liftBase getCurrentTime
            pure $ fst $ root (KBucket time ns', ctx)

--------------------------------------------------------------------------------
-- Sending
--------------------------------------------------------------------------------
-- | Generate a fresh 'TransactionId'.
freshTId ∷ MonadBase IO m ⇒ PendingMap m → m TransactionId
freshTId pending = do
  tId ← randomTId
  if tId `elem` (map fst . Map.keys $ pending)
    then freshTId pending
    else pure tId

-- | Send a generic 'Query'.
sendQuery ∷ MonadBaseControl IO m
  ⇒ SockAddr
  → Query
  → Manager m
  → Callback m
  → ErrorCallback m
  → TimeoutCallback m
  → m ()
sendQuery addr query m cb errorCb timeoutCb = do
  let Manager { managerSettings = ManagerSettings {..}, .. } = m
  (time, tId) ← modifyMVar managerPendingVar $ \pending → do
    tId ← freshTId pending
    time ← liftBase getCurrentTime
    pure (Map.insert (tId, addr) (time, cb, errorCb, timeoutCb) pending, (time, tId))
  let msg = Message (MessageHeader tId managerVersion) (Query managerMyId query)
  void $ liftBase $ SocketBS.sendTo managerSock (LBS.toStrict . encode $ msg) addr

----------------------------------------
-- | Callback for an inappropriate response to our query.
type InvalidCallback m = Dict → String → m ()

--------------------
data PingResponse = PingResponse NodeId

parsePingResponse ∷ Dict → Parser PingResponse
parsePingResponse args = PingResponse <$> args .: "id"

type PingCallback m = PingResponse → m ()

-- | Send a ping query.
sendPing ∷ MonadBaseControl IO m
  ⇒ SockAddr
  → Manager m
  → PingCallback m
  → ErrorCallback m
  → InvalidCallback m
  → TimeoutCallback m
  → m ()
sendPing addr m@(Manager {..}) cb errorCb invalidCb timeoutCb = do
  let query = Ping
      genCb args = either (invalidCb args) cb . parse . parsePingResponse $ args
  sendQuery addr query m genCb errorCb timeoutCb

-- No integration of reply
-- | Ping an address.
ping_ ∷ MonadBaseControl IO m ⇒ SockAddr → Manager m → m (Maybe PingResponse)
ping_ addr m = do
  resultVar ← newEmptyMVar
  sendPing addr m
    (\nId → putMVar resultVar (Just nId))
    (\_ _ → putMVar resultVar Nothing)
    (\_ _ → putMVar resultVar Nothing)
    (putMVar resultVar Nothing)
  pure =<< takeMVar resultVar

-- No integration of reply
-- | Ping a 'Node'.
pingNode_ ∷ MonadBaseControl IO m ⇒ Node → Manager m → m (Maybe PingResponse)
pingNode_ n = ping_ (SockAddrInet (nodePort n) (nodeAddress n))

----------------------------------------
data FindNodeResponse = FindNodeResponse NodeId [Node]
  deriving
    (Eq, Show)

parseFindNodeResponse ∷ Dict → Parser FindNodeResponse
parseFindNodeResponse args =
  FindNodeResponse <$> args .: "id"
                   <*> (map compactNodeToNode <$> args .: "nodes")

type FindNodeCallback m = FindNodeResponse → m ()

-- | Send a find_node.
sendFindNode ∷ MonadBaseControl IO m
  ⇒ SockAddr
  → NodeId -- ^ Id of the node to find.
  → Manager m
  → FindNodeCallback m
  → ErrorCallback m
  → InvalidCallback m
  → TimeoutCallback m
  → m ()
sendFindNode addr targetId m@(Manager {..}) cb errorCb invalidCb timeoutCb =
  let q = FindNode targetId
      genCb args = either (invalidCb args) cb . parse . parseFindNodeResponse $ args
  in sendQuery addr q m genCb errorCb timeoutCb

----------------------------------------
-- There is an unofficial extension to include nodes in a "successful" peers response.
-- Some nodes seem not to include a token in response to a get_peers (e.g. router.bittorrent.com)
data GetPeersResponse
  = GetPeersResponse_Peers NodeId (Maybe Token) [Peer] (Maybe [Node])
  | GetPeersResponse_Nodes NodeId (Maybe Token) [Node]
  deriving
    (Eq, Show)

parseGetPeersResponse ∷ Dict → Parser GetPeersResponse
parseGetPeersResponse args = do
  sourceId ← args .: "id"
  mToken ← args .:? "token"
  fmap (map compactPeerToPeer) <$> args .:? "values" >>= \case
    Nothing → GetPeersResponse_Nodes sourceId mToken
      <$> (map compactNodeToNode <$> args .: "nodes")
    Just ps → GetPeersResponse_Peers sourceId mToken ps
      <$> (fmap (map compactNodeToNode) <$> args .:? "nodes")

type GetPeersCallback m = GetPeersResponse → m ()

-- | Send a get_peers.
sendGetPeers ∷ MonadBaseControl IO m
  ⇒ SockAddr
  → InfoHash -- ^ 'InfoHash' of the torrent to find peers for.
  → Manager m
  → GetPeersCallback m
  → ErrorCallback m
  → InvalidCallback m
  → TimeoutCallback m
  → m ()
sendGetPeers addr ihash m@(Manager {..}) cb errorCb invalidCb timeoutCb = do
  let query = GetPeers ihash
      genCb args = either (invalidCb args) cb . parse . parseGetPeersResponse $ args
  sendQuery addr query m genCb errorCb timeoutCb

----------------------------------------
data AnnouncePeerResponse = AnnouncePeerResponse NodeId
  deriving
    (Eq, Show)

parseAnnouncePeerResponse ∷ Dict → Parser AnnouncePeerResponse
parseAnnouncePeerResponse args = AnnouncePeerResponse <$> args .: "id"

type AnnouncePeerCallback m = AnnouncePeerResponse → m ()

-- | Send an announce_peer.
sendAnnouncePeer ∷ MonadBaseControl IO m
  ⇒ SockAddr
  → InfoHash -- ^ 'InfoHash' to announce for.
  → (Maybe PortNumber)
  → Token -- ^ 'Token' obtained from a previous get_peers.
  → Manager m
  → AnnouncePeerCallback m
  → ErrorCallback m
  → InvalidCallback m
  → TimeoutCallback m
  → m ()
sendAnnouncePeer addr ihash mPort token m@(Manager {..}) cb errorCb invalidCb timeoutCb =
  let query = AnnouncePeer ihash mPort token
      genCb args = either (invalidCb args) cb . parse . parseAnnouncePeerResponse $ args
  in sendQuery addr query m genCb errorCb timeoutCb

--------------------------------------------------------------------------------
-- GenFind
--------------------------------------------------------------------------------
-- FindNode/GetPeers use essentially the same algorithm,
-- so they are rolled into one in this genFind

----------------------------------------
-- CandidateNodeSet
----------------------------------------
-- This is used to keep the set of nodes discovered during genFind

-- | Isomorphic to ('Node', a)
data MarkedNode a = MarkedNode
  { mnNode ∷ Node
  , mnMark ∷ a
  }
  deriving
    (Show)

instance Eq (MarkedNode a) where
  MarkedNode n0 _ == MarkedNode n1 _ = n0 == n1

instance Ord (MarkedNode a) where
  compare (MarkedNode n0 _) (MarkedNode n1 _) = compare n0 n1

instance ToKey (MarkedNode a) where
  toKey = toKey . mnNode

-- | Set of ('MarkedNode' ('Bool', 'Maybe' 'Token')).
--   Ordered by their distance to a fixed 'Key'.
--
-- This is used to keep a set of discovered 'Node's during 'genFind'.
data CandidateNodeSet = CandidateNodeSet
  { cnsOriginKey ∷ Key
  , cnsData ∷ Map Natural (MarkedNode (Bool, Maybe Token))
  }
  deriving
    (Show)

-- | Converts an absolute key to a relative key (distance from originKey).
cnsToKey ∷ ToKey a ⇒ a → CandidateNodeSet → Natural
cnsToKey key cns = distance key (cnsOriginKey cns)

-- | Nodes in the set, in ascending order.
cnsElems ∷ CandidateNodeSet → [MarkedNode (Bool, Maybe Token)]
cnsElems = Map.elems . cnsData

-- | Makes a 'CandidateNodeSet' from a set of appropriately marked 'Node's.
cnsFromSet ∷ (ToKey a) ⇒ a → Set (MarkedNode (Bool, Maybe Token)) → CandidateNodeSet
cnsFromSet originKey = CandidateNodeSet (toKey originKey)
                     . Map.fromDistinctAscList
                     . sortOn fst
                     . map (\mN → (distance originKey mN, mN))
                     . Set.toList

-- | Returns the node closest to 'originKey' and the 'CandidateNodeSet' without this node.
cnsMinView ∷ CandidateNodeSet → Maybe (MarkedNode (Bool, Maybe Token), CandidateNodeSet)
cnsMinView cns = fmap (\(n, dat) → (n, CandidateNodeSet (cnsOriginKey cns) dat))
               . Map.minView
               $ cnsData cns

-- | Lookup a 'Node' in a 'CandidateNodeSet'.
cnsLookup ∷ Node → CandidateNodeSet → Maybe (MarkedNode (Bool, Maybe Token))
cnsLookup n cns = Map.lookup (cnsToKey n cns) (cnsData cns)

-- | Insert a 'Node' into a 'CandidateNodeSet'.
cnsInsert ∷ Node → Maybe Token → CandidateNodeSet → CandidateNodeSet
cnsInsert n mToken cns
  = cns { cnsData = Map.alter (Just . f) (cnsToKey n cns) (cnsData cns) }
  where
    f Nothing = MarkedNode n (False, mToken)
    f (Just (MarkedNode n' (wasQueried, mToken')))
      = MarkedNode n (wasQueried, if isNothing mToken then mToken' else mToken)

-- | The k nodes closest to the 'originKey'.
cnsClosestK
  ∷ Int -- ^ k
  → CandidateNodeSet
  → [MarkedNode (Bool, Maybe Token)]
cnsClosestK k = take k . cnsElems

-- | A 'CandidateNodeSet' is finished if the k closest nodes have all been queried.
cnsIsFinished
  ∷ Int -- ^ k parameter as in Kademlia.
  → CandidateNodeSet
  → Bool
cnsIsFinished k = all (fst . mnMark) . cnsClosestK k

-- | Returns the 'Node' we should query next.
cnsNextToQuery ∷ CandidateNodeSet → Maybe Node
cnsNextToQuery cns = case cnsMinView cns of
  Nothing → Nothing
  Just (MarkedNode n (False, _), _) → Just n
  Just (MarkedNode _ (True, _), cns') → cnsNextToQuery cns'

-- | The k nodes closest to the 'originKey'.
cnsResults ∷ Int → CandidateNodeSet → [Node]
cnsResults k = map mnNode . cnsClosestK k

-- | Mark this 'Node' as queried.
cnsMarkQueried ∷ Node → CandidateNodeSet → CandidateNodeSet
cnsMarkQueried n cns =
  cns { cnsData = Map.adjust f (cnsToKey n cns) (cnsData cns) }
  where
    f (MarkedNode n (_, mToken)) = MarkedNode n (True, mToken)

-- | Number of nodes in this 'CandidateNodeSet'.
cnsSize ∷ CandidateNodeSet → Int
cnsSize = Map.size . cnsData

----------------------------------------
-- | Combined response to a FindNode/GetPeers
data GenFindResponse
  -- There is an unofficial extension to include nodes in a "successful" peers response.
  -- Some nodes seem to not include a token in response to a get_peers (e.g. router.bittorrent.com)
  = GenFindResponse NodeId (Maybe Token) [Peer] (Maybe [Node])
  | GenFindDiscard
  deriving
    (Eq, Show)

findNodeToGenFind ∷ FindNodeResponse → GenFindResponse
findNodeToGenFind (FindNodeResponse sourceId ns) =
  GenFindResponse sourceId Nothing [] (Just ns)

getPeersToGenFind ∷ GetPeersResponse → GenFindResponse
getPeersToGenFind = \case
  GetPeersResponse_Peers sourceId mToken ps mNs →
    GenFindResponse sourceId mToken ps mNs
  GetPeersResponse_Nodes sourceId mToken ns →
    GenFindResponse sourceId mToken [] (Just ns)

-- | State kept during genFind
type GenFindState =
  (Set Peer, TQueue (Node, GenFindResponse), Int, CandidateNodeSet)

type GenFindT m a = StateT GenFindState m a

getQueue ∷ Monad m ⇒ GenFindT m (TQueue (Node, GenFindResponse))
getQueue = view _2 <$> get

getPendingCount ∷ Monad m ⇒ GenFindT m Int
getPendingCount = view _3 <$> get

getCNS ∷ Monad m ⇒ GenFindT m CandidateNodeSet
getCNS = view _4 <$> get

-- | Reads one item from results queue.
waitResult ∷ MonadBaseControl IO m ⇒ GenFindT m (Node, GenFindResponse)
waitResult = do
  mR ← liftBase . atomically . readTQueue =<< getQueue
  modify (_3 %~ pred)
  pure mR

-- | Tries to read one item from the results queue.
tryReadResult ∷ MonadBaseControl IO m ⇒ GenFindT m (Maybe (Node, GenFindResponse))
tryReadResult = do
  mR ← liftBase . atomically . tryReadTQueue =<< getQueue
  when (isJust mR) $ do
    modify (_3 %~ pred)
  pure mR

-- | Sends a find_node.
fireFindNode ∷ MonadBaseControl IO m
  ⇒ Node -- ^ Destination 'Node'.
  → NodeId -- ^ Id of the node to find.
  → Manager m
  → GenFindT m ()
fireFindNode n@(Node _ address port) targetId m = do
  modify (_3 %~ succ)
  queue ← getQueue
  let addr = (SockAddrInet port address)
      cb = liftBase . atomically . writeTQueue queue . (n,) . findNodeToGenFind
      timeoutCb = liftBase . atomically . writeTQueue queue $ (n, GenFindDiscard)
      errorCb = \_ _ → timeoutCb
      invalidCb = \_ _→ timeoutCb
  lift $ sendFindNode addr targetId m cb errorCb invalidCb timeoutCb

-- | Sends a get_peers.
fireGetPeers ∷ MonadBaseControl IO m
  ⇒ Node -- ^ Destination 'Node'.
  → InfoHash -- ^ 'InfoHash' of the torrent to get peers for.
  → Manager m
  → GenFindT m ()
fireGetPeers n@(Node _ address port) ihash m = do
  modify (_3 %~ succ)
  queue ← getQueue
  let addr = SockAddrInet port address
      cb = liftBase . atomically . writeTQueue queue . (n,) . getPeersToGenFind
      timeoutCb = liftBase . atomically . writeTQueue queue $ (n, GenFindDiscard)
      errorCb = \_ _ → timeoutCb
      invalidCb = \_ _→ timeoutCb
  lift $ sendGetPeers addr ihash m cb errorCb invalidCb timeoutCb

-- | Runs the full find_node or get_peers algorithm, depending on the second argument.
--
-- Since the algorithms for these are so similar, they are combined into one here.
genFind ∷ ∀ a m. (ToKey a, MonadBaseControl IO m)
  ⇒ a -- ^ The node or infohash to find or get peers for.
  → (Node → GenFindT m ()) -- ^ Fire a query of the appropriate type.
  → Manager m
  → m GenFindState
genFind key fireQuery m = do
  resultsQueue ← liftBase newTQueueIO
  initialCNS ← do
    table ← readManagerTable m
    let ns = cnsFromSet key
           . Set.map (flip MarkedNode (False, Nothing))
           $ closestNodes k key table
    pure ns
  let initialState = (Set.empty, resultsQueue, 0, initialCNS)
  execStateT go initialState
  where
    k = managerK $ managerSettings m
    α = managerAlpha $ managerSettings m
    markQueried ∷ Node → GenFindT m ()
    markQueried n = modify (_4 %~ cnsMarkQueried n)
    log ∷ String → GenFindT m ()
    log str = do
      (_, _, pendingCount, cns) ← get
      let strs ∷ [String] = [ "genFind (", show pendingCount, ") ("
                            , show $ cnsSize cns, "): "
                            , str
                            ]
      lift $ managerLog (managerSettings m) $ concat strs
    ----------
    isFinished ∷ GenFindT m Bool
    isFinished = cnsIsFinished k <$> getCNS
    nextToQuery ∷ GenFindT m (Maybe Node)
    nextToQuery = cnsNextToQuery <$> getCNS
    -------------------- States
    go = do
      log "go"
      isFinished >>= bool tryRead wait
    wait = do
      log "wait"
      pendingCount ← getPendingCount
      if pendingCount == 0
        then finish
        else waitResult >>= integrate
    tryRead = tryReadResult >>= \case
        Nothing → do
          log "tryRead: queue empty"
          tryFire
        Just r → do
          log "tryRead: read ok"
          integrate r
    integrate (n, r) = do
      lift $ fork $ integrateNode n m
      case r of
        GenFindResponse sourceId mToken ps mNs → do
          if sourceId == nodeId n
            then do
            modify (_4 %~ cnsInsert n mToken) -- Updating token
            integratePeers ps
            case mNs of
              Nothing → do
                log $ "integrate: firing find_node to " ++ show n
                fireFindNode n (NodeId $ unKey . toKey $ key) m
              Just ns → integrateNodes ns
            else
            log $ "integrate: unexpected id from" ++ show n
        GenFindDiscard → log $ "integrate: discarded from " ++ show n
      go
      where
        integratePeers ps = do
          modify (_1 %~ (<> (Set.fromList ps)))
          log $ "integrate: integrated peers from " ++ show n
        integrateNodes ns = do
          modify (_4 %~ (\cns → foldl (\cns' n → cnsInsert n Nothing cns') cns ns))
          log $ "integrate: integrated nodes from " ++ show n
    tryFire = nextToQuery >>= \case
      Nothing → do
        log $ "tryFire: out of nodes"
        wait
      Just n → do
        log $ "tryFire: firing query to " ++ show n
        fireQuery n
        markQueried n
        go
    finish = do
      log "finish"

--------------------------------------------------------------------------------
-- Bootstrap
--------------------------------------------------------------------------------
-- | Bootstrap a 'Manager'.
--
-- Pings the bootstrap nodes to obtain their 'NodeId''s and integrates them.
bootstrap ∷ MonadBaseControl IO m ⇒ Manager m → m ()
bootstrap m@(Manager { managerSettings = ManagerSettings {..}, .. }) = do
  managerLog "bootstrap: started"
  ns ← fmap catMaybes
     . mapConcurrently (checkAddr managerBootstrapTries)
     $ managerBootstrapNodes 
  traverse_ (flip integrateNode m) ns
  where
    checkAddr 0 addr = do
      managerLog $ "bootstrap: giving up on " ++ show addr 
      pure Nothing
    checkAddr n addr@(SockAddrInet port address) = do
      managerLog $ "bootstrap: pinging " ++ show addr 
      ping_ addr m >>= \case
        Nothing → do
          managerLog $ "bootstrap: ping failed with " ++ show addr 
          checkAddr (pred n) addr
        Just (PingResponse nId) → do
          managerLog $ "bootstrap: ping succeeded with " ++ show addr 
          pure $ Just $ Node nId address port
