{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main ( main ) where

import System.Environment (getArgs)
import Control.Exception as E
import Control.Distributed.Process
import Control.Distributed.Process.Internal.Types (LocalNode)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (runProcess,forkProcess,initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
-- import qualified Data.Map as M
import qualified Data.IntMap as M
import qualified Data.List as L
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.List.Split
import Data.Typeable (Typeable)
import Data.Binary (Binary (get,put),putWord8,getWord8)
import Data.Maybe

type Key    = Int
type Value  = B.ByteString
type ProcessName = ProcessId
type KeyValueStore = M.IntMap Value

data Slice = Slice 
    { process   :: ProcessName
    , keyStart  :: Key
    , keyStop   :: Key
    } deriving (Show,Eq)

data Cache = Cache 
    { pairs    :: KeyValueStore
    , slices   :: [Slice]
    } deriving (Show)

addSlice :: Cache -> Slice -> Cache
addSlice c s = c { slices = [s] `L.union` slices c }

removeAllSlicesForPid :: Cache -> ProcessName -> Cache
removeAllSlicesForPid c pid = c { slices = L.filter f (slices c) }
        where f slice = process slice /= pid

addPair :: Cache -> Key -> Value -> Cache
addPair c k v = c { pairs = M.insert k v (pairs c) }

sliceHandleKey :: Key -> Slice -> Bool
sliceHandleKey k slice  =  (k >= keyStart slice) && (k < keyStop slice)

type CacheState = TVar Cache

data WaitingState = WaitingState
    deriving (Show)

type WaitingFor a = Either () a

data SliceUpdate = Claim ProcessId Key Key
    deriving (Show, Typeable)

data PairRequest = Get ProcessId Key 
    | Set ProcessId Key Value
    deriving (Show, Typeable)

data PairReply = RGet Key (Maybe Value)
    | RSet ProcessId
    deriving (Show, Typeable)

data MonitorMessage = Ping ProcessId
    | Pong ProcessId
    | Dump
    deriving (Show, Typeable)

instance Binary PairRequest where
    put (Get pid k)   = do putWord8 0; put (pid,k)
    put (Set pid k v) = do putWord8 2; put (pid, k, v)
    get = do
        header <- getWord8
        case header of
          0 -> do (pid,k)   <- get; return (Get pid k)
          2 -> do (pid,k,v) <- get; return (Set pid k v)
          _ -> fail "PairRequest.get: invalid"

instance Binary PairReply where
    put (RGet k v)    = do putWord8 1; put (k, v)
    put (RSet pid)    = do putWord8 3; put pid
    get = do
        header <- getWord8
        case header of
          1 -> do (k,v)     <- get; return (RGet k v)
          3 -> do (pid)     <- get; return (RSet pid)
          _ -> fail "PairReply.get: invalid"

instance Binary SliceUpdate where
    put (Claim pid k1 k2) = do putWord8 0; put (pid,k1,k2)
    get = do
        header <- getWord8
        case header of
          0 -> do (p,k1,k2)   <- get; return (Claim p k1 k2)
          _ -> fail "SliceUpdate.get: invalid"

instance Binary MonitorMessage where
    put (Ping pid)    = do putWord8 1; put pid
    put (Pong pid)    = do putWord8 2; put pid
    put Dump          = putWord8 3
    get = do
        header <- getWord8
        case header of
          1 -> do (pid)     <- get; return (Ping pid)
          2 -> do (pid)     <- get; return (Pong pid)
          3 -> return Dump
          _ -> fail "MonitorMessage.get: invalid"

setKey :: ProcessId -> Key -> Value -> Process ProcessId
setKey me k v = do
    {-# SCC "setsend" #-}nsend "pairs.server" $ Set me k v
    go
    where go = do msg <- {-# SCC "setexpect" #-}expect
                  case msg of
                        RSet keyPid     -> return keyPid
                        _               -> fail "expecting a RSet" 

getKey :: ProcessId -> Key -> Process (Maybe Value) 
getKey me k = do 
    {-# SCC "getsend" #-} nsend "pairs.server" $ Get me k
    go
    where go = do msg <- {-# SCC "getexpect" #-}expect
                  case msg of
                        RGet k val      -> return val
                        _               -> fail "expecting a RGet"

setKey' :: CacheState -> ProcessId -> Key -> Value -> Process (WaitingFor ProcessId)
setKey' state requester k val = do
    cache <- {-# SCC "tvarset" #-} liftIO $ atomically $ readTVar state
    let slice = L.find (sliceHandleKey k) $ slices cache
    case slice of
        Nothing -> liftM Right (({-# SCC "tvarmodify" #-} liftIO $ atomically $ modifyTVar state f) >> getSelfPid)
                        where f st0 = addPair st0 k val
        Just sl -> liftM Left (storeKey requester k val $ process sl)

getKey' :: CacheState -> ProcessId -> Key -> Process (WaitingFor  (Maybe Value)) 
getKey' state requester k = do
    cache <- {-# SCC "tvarget" #-}liftIO $ atomically $ readTVar state
    let val = M.lookup k $ pairs cache
    case val of 
        (Just _) -> return $ Right val
        Nothing  -> do
            let slice = L.find (sliceHandleKey k) $ slices cache
            case slice of
                Nothing -> return $ Right Nothing
                Just sl -> liftM Left (retrieveKey requester k $ process sl)

storeKey :: ProcessId -> Key -> Value -> ProcessId -> Process ()
storeKey requester k val pid = send pid $ Set requester k val

retrieveKey :: ProcessId -> Key -> ProcessId -> Process ()
retrieveKey requester k pid = send pid $ Get requester k

pairsManager :: CacheState -> Process ()
pairsManager state = do
    getSelfPid >>= register "pairs.server"
    forever $ do
        msg <- expect
        case msg of
            -- if we don't have an immediate result, ignore, it has been forwarded
            Get requester k -> getKey' state requester k >>= either ignore reply
                where reply    = send requester . RGet k
                      ignore _ = return ()

            Set requester k val -> setKey' state requester k val >>= either ignore reply
                where reply    = send requester . RSet
                      ignore _ = return ()

sliceManager :: CacheState -> Process ()
sliceManager state = do
    getSelfPid >>= register "slice.server"
    liftIO $ print "slice registered"
    forever $ receiveWait   [ match (remoteSliceDied state) , 
                              match (sliceClaimed state)
                            ]
monitoringManager :: CacheState -> Process ()
monitoringManager state = do
    getSelfPid >>= register "monitor.server"
    liftIO $ print "monitor registered"
    me <- getSelfPid
    forever $ do
        msg <- expect
        case msg of
            Ping from -> send from $ Pong me
            Dump      -> liftIO $ atomically (readTVar state) >>= print

remoteSliceDied :: CacheState -> ProcessMonitorNotification -> Process ()
remoteSliceDied state (ProcessMonitorNotification _ pid reason) = 
    liftIO $ atomically $ modifyTVar state f 
            where f st0 = removeAllSlicesForPid st0 pid

sliceClaimed :: CacheState -> SliceUpdate -> Process ()
sliceClaimed state (Claim pid k0 k1) = do
    monitor pid -- XXX this will setup multiple monitors for same process
    liftIO $ atomically $ modifyTVar state f 
            where f st0 = addSlice st0 $ Slice pid k0 k1


monitorMessage :: MonitorMessage -> Process ()
monitorMessage = nsend "monitor.server"

sliceMessage :: SliceUpdate -> Process ()
sliceMessage = nsend "slice.server"

remotable ['sliceMessage,'monitorMessage]
remotables = __remoteTable initRemoteTable

forwardSliceUpdate :: SliceUpdate -> NodeId -> Process ()
forwardSliceUpdate msg n = void (spawn n ($(mkClosure 'sliceMessage) msg))

runPort :: String -> [(Key,Key)] -> IO ()
runPort port ks = do
    print ks 
    --backend
    b <- initializeBackend "localhost" port remotables 
    
    -- localnodes storing data slices
    localNodes <- mapM (\_ -> newLocalNode b) ks
    mapM_ (\(kPair,n) -> forkIO (runBackend kPair n b)) (zip ks localNodes)

    -- channel to receive command to be parsed
    tc <- atomically newTChan :: IO (TChan B.ByteString)

    -- localnode doing the parsing is the node for the first slice
    let me = head localNodes
    forkProcess me $ forever $ handleString =<< liftIO (atomically $ readTChan tc)

     -- XXX this is an horrible hack to allow piping in stdin after some of time
    threadDelay 3000000 
    print "reading stdin"
    E.catch (forever (B.getLine >>= \bs -> atomically (writeTChan tc bs))) ignore
    print "done"
            where ignore :: IOException -> IO ()
                  ignore _ = return ()


runBackend :: (Key,Key) -> LocalNode -> Backend -> IO ()
runBackend (k0,k1) me backend = do
    cache <- atomically $ newTVar $ Cache M.empty []
    pairsMan <- forkProcess me $ pairsManager cache
    forkProcess me $ claimSliceToOtherNodes (Slice pairsMan k0 k1) backend
    forkProcess me $ sliceManager cache
    forkProcess me $ monitoringManager cache
    return ()


handleString :: B.ByteString -> Process ()
handleString str = {-# SCC "handleString" #-} do 
    this <- getSelfPid
    ret <- case ({-# SCC "splitOn" #-} C.split ' ' str) of
           ["dump"]     -> monitorMessage Dump     >>  return Nothing
           ["get",k]    -> liftM (Just . show) (getKey this (read' k))
           ["set",k,v]  -> liftM (Just . show) (setKey this (read' k) v)
           _            -> return $ Just "not understood"
    liftIO $ maybe (return ()) putStrLn ret
    where read' = fst . fromJust . C.readInt

claimSliceToOtherNodes :: Slice -> Backend -> Process ()
claimSliceToOtherNodes (Slice pid k0 k1) backend =
    forever $ {-# SCC "claimpeers" #-} do
        nodes <- liftIO $ findPeers backend 500000
        me <- getSelfNode
        forM_ (filter (/= me) nodes) (forwardSliceUpdate (Claim pid k0 k1))

main :: IO ()
main = do
    args <- getArgs
    case args of
        port:[] -> runPort port [(0,65535)]
        port:skps -> runPort port pairs
                    where pairs = map toKeyPair skps
                                    where toKeyPair :: String -> (Key,Key)
                                          toKeyPair s = head $ zip ks (tail ks)
                                                          where ks = map read $ splitOn "," s
