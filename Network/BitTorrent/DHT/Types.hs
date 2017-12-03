{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE ViewPatterns #-}

module Network.BitTorrent.DHT.Types
  ( TransactionId (..)

  , NodeId (..)

  , Token (..)

  , Node (..)

  , CompactPeer (..), compactPeerToPeer, peerToCompactPeer
  , CompactNode (..), compactNodeToNode, nodeToCompactNode

  , Key (..)
  , ToKey (..)
  , isPrefixOf
  , distance

  , ErrorCode (..)
  , Message (..)
  , MessageHeader (..)
  , MessageBody (..)
  , Query (..)
  )
where

import Control.Lens (_1, _3, view)
import Control.Monad ((>=>), liftM4)
import Data.BEncode
import Data.Bits (xor)
import Data.Binary (Binary)
import qualified Data.Binary as Bin (Binary (..), decodeOrFail, encode)
import qualified Data.Binary.Builder as Bin (toLazyByteString)
import qualified Data.Binary.Get as Bin (getByteString, getWord16be, getWord8)
import qualified Data.Binary.Put as Bin (execPut, putByteString, putWord16be, putWord8)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS (length, splitAt)
import qualified Data.ByteString.Lazy as LBS (fromStrict, toStrict)
import Data.List (intersperse)
import qualified Data.Map as Map (fromList)
import Data.Monoid ((<>))
import Data.String (IsString)
import Data.Traversable (for)
import qualified Data.Vector.Unboxed as VU (Vector, (!), length, zipWith)
import Data.Word (Word8)
import Network.BitTorrent.Types
import Network.Socket (HostAddress, PortNumber, hostAddressToTuple, tupleToHostAddress)
import Numeric.Natural (Natural)

--------------------------------------------------------------------------------
-- | Transaction ID for a Message.
newtype TransactionId = TransactionId ByteString
  deriving
    (Eq, FromBEncode, IsString, Ord, Show, ToBEncode)

--------------------------------------------------------------------------------
newtype NodeId = NodeId { unNodeId ∷ VU.Vector Bool } -- 160bit big endian
  deriving
    (Eq, Ord, Show)

instance Binary NodeId where
  get = NodeId . bsToBEBitField <$> Bin.getByteString 20
  put (NodeId nId) = Bin.putByteString $ beBitFieldToBS nId

instance FromBEncode NodeId where
  parseBEncode (String s) =
    either (fail . view _3) (pure . view _3) $ Bin.decodeOrFail $ LBS.fromStrict s
  parseBEncode _ = fail "NodeId: expected String"

instance ToBEncode NodeId where
  toBEncode = String . LBS.toStrict . Bin.encode

--------------------------------------------------------------------------------
-- | Token for getPeers and announcePeer.
newtype Token = Token ByteString
  deriving
    (Eq, FromBEncode, IsString, Ord, Show, ToBEncode)

--------------------------------------------------------------------------------
instance FromBEncode PortNumber where
  parseBEncode = fmap fromInteger . parseBEncode

instance ToBEncode PortNumber where
  toBEncode = toBEncode . toInteger

--------------------------------------------------------------------------------
data Node = Node
  { nodeId ∷ NodeId
  , nodeAddress ∷ HostAddress
  , nodePort ∷ PortNumber
  }
  deriving
    (Show)

instance Eq Node where
  n0 == n1 = nodeId n0 == nodeId n1

instance Ord Node where
  compare n0 n1 = compare (nodeId n0) (nodeId n1)

--------------------------------------------------------------------------------
-- | Compact Node Information.
data CompactNode = CompactNode
  { cnodeId ∷ NodeId
  , cnodeAddress ∷ HostAddress
  , cnodePort ∷ PortNumber
  }
  deriving
    (Show)

instance Binary CompactNode where
  get = do
    nId ← Bin.get
    addr ← liftM4 (,,,) Bin.getWord8 Bin.getWord8 Bin.getWord8 Bin.getWord8
    port ← fromIntegral <$> Bin.getWord16be
    pure $ CompactNode nId (tupleToHostAddress addr) port
  put (CompactNode nId addr port) = do
    Bin.put nId
    let (addr3, addr2, addr1, addr0) = hostAddressToTuple addr
    Bin.putWord8 addr3
    Bin.putWord8 addr2
    Bin.putWord8 addr1
    Bin.putWord8 addr0
    Bin.putWord16be $ fromIntegral port

instance Eq CompactNode where
  n0 == n1 = cnodeId n0 == cnodeId n1

instance Ord CompactNode where
  compare n0 n1 = compare (cnodeId n0) (cnodeId n1)

instance {-# OVERLAPPING #-} FromBEncode [CompactNode] where
  parseBEncode (String bs)
    | (BS.length bs `rem` 26 == 0) =
        let chunk k bs
              | (BS.length bs == 0) = k []
              | otherwise = let (c, bs') = BS.splitAt 26 bs
                            in chunk (k . (c :)) bs'
        in for (chunk id bs) $ \c → case Bin.decodeOrFail (LBS.fromStrict c) of
          Left (_, _, err) → fail $ "[CompactNode]: " <> err
          Right (_, _, node) → pure node
    | otherwise =
        fail "[CompactNode]: Length not divisible by 26"
  parseBEncode _ = fail "[CompactNode]: Expected String"

instance {-# OVERLAPPING #-} ToBEncode [CompactNode] where
  toBEncode = String
            . LBS.toStrict
            . Bin.toLazyByteString
            . mconcat
            . map (Bin.execPut . Bin.put)

compactNodeToNode ∷ CompactNode → Node
compactNodeToNode (CompactNode nId addr port) = Node nId addr port

nodeToCompactNode ∷ Node → CompactNode
nodeToCompactNode (Node nId addr port) = CompactNode nId addr port

--------------------------------------------------------------------------------
-- | Compact Peer Information.
data CompactPeer = CompactPeer HostAddress PortNumber
  deriving
    (Eq, Show)

instance Binary CompactPeer where
  get = do
    addr ← liftM4 (,,,) Bin.getWord8 Bin.getWord8 Bin.getWord8 Bin.getWord8
    port ← fromIntegral <$> Bin.getWord16be
    pure $ CompactPeer (tupleToHostAddress addr) port
  put (CompactPeer addr port) = do
    let (addr3, addr2, addr1, addr0) = hostAddressToTuple addr
    Bin.putWord8 addr3
    Bin.putWord8 addr2
    Bin.putWord8 addr1
    Bin.putWord8 addr0
    Bin.putWord16be $ fromIntegral port

instance FromBEncode CompactPeer where
  parseBEncode (String s) = case Bin.decodeOrFail (LBS.fromStrict s) of
    Left (_, _, err) → fail $ "CompactPeer: " <> err
    Right (_, _, p) → pure p
  parseBEncode _ = fail "CompactPeer: expected String"

instance ToBEncode CompactPeer where
  toBEncode = String . LBS.toStrict . Bin.encode

compactPeerToPeer ∷ CompactPeer → Peer
compactPeerToPeer (CompactPeer addr port) = Peer addr port Nothing

peerToCompactPeer ∷ Peer → CompactPeer
peerToCompactPeer (Peer addr port _) = CompactPeer addr port

--------------------------------------------------------------------------------
-- | This represents a key in the DHT.
-- Usually a 'InfoHash', 'NodeId' or 'Node' (see 'ToKey').
newtype Key = Key { unKey ∷ VU.Vector Bool } -- 160bit big endian
  deriving
    (Eq, Ord, Show)

class ToKey k where
  toKey ∷ k → Key

  vuIsPrefixOf ∷ VU.Vector Bool → k → Bool
  vuIsPrefixOf x (toKey → Key y) = go 0
    where
      go i | (i >= VU.length x) = True
           | (i >= VU.length y) = False
           | (x VU.! i == y VU.! i) = go (succ i)
           | otherwise = False

  vuDistance ∷ VU.Vector Bool → k → Natural
  vuDistance x (toKey → Key y) = beBitFieldToNatural $ VU.zipWith xor x y

instance ToKey Key where
  toKey = id

instance ToKey InfoHash where
  toKey = Key . unInfoHash

instance ToKey NodeId where
  toKey = Key . unNodeId

instance ToKey Node where
  toKey = toKey . nodeId

isPrefixOf ∷ (ToKey a, ToKey b) ⇒ a → b → Bool
isPrefixOf (toKey → Key x) (toKey → y) = vuIsPrefixOf x y

distance ∷ (ToKey a, ToKey b) ⇒ a → b → Natural
distance (toKey → Key x) b = vuDistance x b

--------------------------------------------------------------------------------
-- | Error codes for an error 'Message'.
data ErrorCode
  = GenericError
  | ServerError
  | ProtocolError
  | MethodUnknown
  deriving
    (Eq, Show)

instance FromBEncode ErrorCode where
  parseBEncode (Integer i) = case i of
    201 → pure GenericError
    202 → pure ServerError
    203 → pure ProtocolError
    204 → pure MethodUnknown
    _ → fail "ErrorCode: unknown error code"
  parseBEncode _ = fail "ErrorCode: Expected Integer"

instance ToBEncode ErrorCode where
  toBEncode = \case
    GenericError → Integer 201
    ServerError → Integer 202
    ProtocolError → Integer 203
    MethodUnknown → Integer 204

-- | A DHT Message.
data Message = Message
  { header ∷ MessageHeader -- ^ Contains info common to all 'Message's.
  , body ∷ MessageBody -- ^ Contains info specific to each 'Message' type.
  }
  deriving
    (Eq, Show)

data MessageHeader = MessageHeader
  { transactionId ∷ TransactionId -- ^ Transaction ID for this 'Message'.
  , version ∷ Maybe ByteString -- ^ Software version of the sender of this 'Message'.
  }
  deriving
    (Eq, Show)

data MessageBody
  = Query NodeId Query
  | Response Dict
  | Error ErrorCode String
  deriving
    (Eq, Show)

data Query
  = Ping
  | FindNode NodeId
  | GetPeers InfoHash
  | AnnouncePeer InfoHash (Maybe PortNumber) Token
  deriving
    (Eq, Show)

--------------------
parseDict ∷ Value → Parser Dict
parseDict (Dict d) = pure d
parseDict _ = fail "Message: Expected a Dict"

parseHeader ∷ Dict → Parser MessageHeader
parseHeader d = MessageHeader <$> d .: "t" <*> d .:? "v"

parseBody ∷ Dict → Parser MessageBody
parseBody d = d .: "y" >>= \(msgType ∷ ByteString) → if
  | msgType == "q" → do
      method ∷ ByteString ← d .: "q"
      args ← d .: "a"
      Query <$> args .: "id" <*> parseQuery method args
  | msgType == "r" →
      Response <$> d .: "r"
  | msgType == "e" → do
      [errorCodeVal, errorStrVal] ∷ [Value] ← d .: "e"
      Error <$> parseBEncode errorCodeVal <*> parseBEncode errorStrVal

parseQuery ∷ ByteString → Dict → Parser Query
parseQuery method args = if
  | method == "ping" → pure Ping
  | method == "find_node" → FindNode <$> args .: "target"
  | method == "get_peers" → GetPeers <$> args .: "info_hash"
  | method == "announce_peer" → do
      ihash ← args .: "info_hash"
      token ← args .: "token"
      mPort ← args .:? "implied_port" >>= \case
        Nothing → Just <$> args .: "port"
        Just (0 ∷ Word8) → Just <$> args .: "port"
        Just (1 ∷ Word8) → pure Nothing
        _ → fail "Message: announce_peer: implied_port has invalid value"
      pure $ AnnouncePeer ihash mPort token
  | otherwise →
      fail "Unknown method"

parseMessage ∷ Dict → Parser Message
parseMessage d = Message <$> parseHeader d <*> parseBody d

instance FromBEncode Message where
  parseBEncode = parseDict >=> parseMessage

--------------------
encodeHeader ∷ MessageHeader → Dict
encodeHeader (MessageHeader tId mVersion) = Map.fromList $
  [("t", toBEncode tId)] ++ maybe [] (pure . ("v",) . String) mVersion

encodeBody ∷ MessageBody → Dict
encodeBody = Map.fromList . \case
  Query (NodeId sourceId) query →
    [ ("y", String "q")
    , ("q", String $ case query of
                       Ping → "ping"
                       FindNode _ → "find_node"
                       GetPeers _ → "get_peers"
                       AnnouncePeer _ _ _ → "announce_peer")
    , ("a", Dict $ Map.fromList [("id", toBEncode $ beBitFieldToBS sourceId)]
                <> encodeQuery query)
    ]
  Response args →
    [ ("y", String "r")
    , ("r", Dict args)
    ]
  Error eCode eStr →
    [ ("y", String "e")
    , ("e", List [toBEncode eCode, toBEncode eStr])
    ]

encodeQuery ∷ Query → Dict
encodeQuery = Map.fromList . \case
  Ping → []
  FindNode targetId → [("target", toBEncode targetId)]
  GetPeers ihash → [("info_hash", toBEncode ihash)]
  AnnouncePeer ihash mPort token →
    [ ("info_hash", toBEncode ihash)
    , ("token", toBEncode token)
    , maybe ("implied_port", Integer 1)
      (\port → ("port", toBEncode port))
      mPort
    ]

instance ToBEncode Message where
  toBEncode (Message header body) = Dict $ encodeHeader header <> encodeBody body
