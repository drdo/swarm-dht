{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE UnicodeSyntax #-}

module Network.BitTorrent.DHT.KBucket
  ( KBucket
  , Table (..)
  , TableCtx (..)
  , TableLoc
  , tableLoc
  , left, right, up, root
  , modifyLoc
  , rootTable
  , lookupBucket
  , subtablePrefix
  , isFull
  , closestNodes
  , DryInsertNodeResult (..)
  , dryInsertNode
  )
where

import Data.List (sortOn)
import Data.Map (Map)
import qualified Data.Map as Map (keys, lookup, partitionWithKey)
import Data.Monoid ((<>))
import Data.Set (Set)
import qualified Data.Set as Set (fromList, size)
import Data.Time.Clock (UTCTime)
import qualified Data.Vector.Unboxed as VU (Vector)
import Network.BitTorrent.DHT.Types
import Network.BitTorrent.Types
import Numeric.Natural (Natural)

--------------------------------------------------------------------------------
-- | A single k-bucket.
-- Each 'Node' is associated with the time it was last seen
-- and how many queries in a row it has failed to answer.
type KBucket = Map Node (UTCTime, Int)

-- | Routing table.
data Table
  = KBucket UTCTime KBucket -- ^ The 'UTCTime' is when this 'KBucket' was last updated.
  | Split Table Table
  deriving
    (Eq, Show)

-- | Context for a 'Table' zipper.
data TableCtx
  = Top
  | L TableCtx Table
  | R Table TableCtx
  deriving
    (Eq, Show)

-- | A location in a 'Table'.
type TableLoc = (Table, TableCtx)

-- | Make a 'TableLoc' from a 'Table'. Positioned at the 'Top'.
tableLoc ∷ Table → TableLoc
tableLoc t = (t, Top)

-- | Go left in a 'Table'. Partial.
left ∷ TableLoc → TableLoc
left (Split l r, ctx) = (l, L ctx r)

-- | Go right in a 'Table'. Partial.
right ∷ TableLoc → TableLoc
right (Split l r, ctx) = (r, R l ctx)


-- | Go up in a 'Table'. Partial.
up ∷ TableLoc → TableLoc
up (t, L ctx r) = (Split t r, ctx)
up (t, R l ctx) = (Split l t, ctx)

-- | Go to the root of this 'Table'.
root ∷ TableLoc → TableLoc
root loc@(_, Top) = loc
root loc = root (up loc)

-- | Swap the subtable at this location.
modifyLoc ∷ Table → TableLoc → TableLoc
modifyLoc t' (t, ctx) = (t', ctx)

-- | The root 'Table' of this location.
--
-- @
-- rootTable = fst . 'root'
-- @
rootTable ∷ TableLoc → Table
rootTable = fst . root

-- | The 'KBucket' this 'NodeId' belongs to.
lookupBucket ∷ NodeId → Table → TableLoc
lookupBucket nId = go [] . tableLoc
  where
    go _ loc@(KBucket _ _, _) = loc
    go prefix t@(Split l r, _)
      | ((prefix <> [False]) `vuIsPrefixOf` nId) = go (prefix <> [False]) (left t)
      | otherwise = go (prefix <> [True]) (right t)

-- | The 'Key' prefix corresponding to this 'Table' location.
subtablePrefix ∷ TableLoc → VU.Vector Bool
subtablePrefix = go . snd
  where
    go Top = ([] ∷ VU.Vector Bool)
    go (L ctx _) = go ctx <> [False]
    go (R _ ctx) = go ctx <> [True]

-- | Tests if a 'KBucket' is full for a given k parameter.
isFull ∷ Int → KBucket → Bool
isFull k = (>= k) . length

-- | The k 'Node's in this 'Table' closest to a given 'Key'.
closestNodes ∷ (ToKey a) ⇒ Int → a → Table → Set Node
closestNodes k key t = go k [] t
  where
    go 0 _ _ = []
    go k _ (KBucket _ ns) =
      Set.fromList . take k . sortOn (distance key) . Map.keys $ ns
    go k prefix (Split l r)
      | vuDistance (prefix <> [False]) key < vuDistance (prefix <> [True]) key =
          let ns = go k (prefix <> [False]) l
              size = Set.size ns
          in ns <> go (k-size) (prefix <> [True]) r
      | vuDistance (prefix <> [False]) key > vuDistance (prefix <> [True]) key = -- Could just be otherwise
          let ns = go k (prefix <> [True]) r
              size = Set.size ns
          in ns <> go (k-size) (prefix <> [False]) l

-- | Result for a 'dryInsertNode'.
data DryInsertNodeResult
  = Successful TableLoc -- ^ The 'Node' can be inserted in this 'KBucket'.
  | AlreadyPresent TableLoc -- ^ The 'Node' was already present in the 'Table'.
  | Full TableLoc -- ^ The 'Node' would be inserted in this 'KBucket', but it is full.
  deriving
    (Eq, Show)

-- | Does a dry insertion of a 'Node', spliting 'KBucket's as appropriate.
-- Does not actually insert the 'Node'.
dryInsertNode ∷ (ToKey a)
  ⇒ Node -- ^ The 'Node' to insert.
  → a -- ^ Our 'Key'. Used to decide when to split 'KBucket's.
  → Int -- ^ k parameter as in Kademlia.
  → Table
  → DryInsertNodeResult
dryInsertNode n originKey k table = go (tableLoc table)
  where
    go loc = case loc of
      (KBucket lastChanged ns, ctx) →
        case Map.lookup n ns of
          Just _ → AlreadyPresent loc
          Nothing →
            if isFull k ns
            then let prefix = subtablePrefix loc
                 in if prefix `vuIsPrefixOf` originKey
                    then let (ls, rs) = Map.partitionWithKey (\n _ → vuIsPrefixOf (prefix <> [False]) n) ns
                         in go (Split (KBucket lastChanged ls) (KBucket lastChanged rs), ctx)
                    else Full loc
            else Successful loc
      (Split _ _, _)
        | subtablePrefix (left loc) `vuIsPrefixOf` n → go (left loc)
        | otherwise → go (right loc)
