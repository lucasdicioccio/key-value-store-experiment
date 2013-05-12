{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Internal.Types (LocalNode)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (runProcess,forkProcess,initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Concurrent.MVar
import Control.Concurrent (threadDelay)
import Control.Monad
import qualified Data.Map as M
import qualified Data.List as L
import Data.Typeable (Typeable)
import Data.Binary (Binary (get,put),putWord8,getWord8)
import Data.Maybe

type Key    = Int
type Value  = String
type ProcessName = ProcessId

data Slice = Slice 
    { process   :: ProcessName
    , keyStart  :: Key
    , keyStop   :: Key
    } deriving (Show,Eq)

data Cache = Cache 
    { pairs    :: M.Map Key Value
    , slices   :: [Slice]
    } deriving (Show)

addSlice :: Cache -> Slice -> Cache
addSlice c s = c { slices = L.union [s] (slices c) }

removeAllSlicesForPid :: Cache -> ProcessName -> Cache
removeAllSlicesForPid c pid = c { slices = L.filter f (slices c) }
        where f slice = process slice /= pid


addPair :: Cache -> Key -> Value -> Cache
addPair c k v = c { pairs = M.insert k v (pairs c) }

sliceHandleKey :: Key -> Slice -> Bool
sliceHandleKey k slice  =  (k >= keyStart slice) && (k < keyStop slice)

type CacheState = MVar Cache

data SliceUpdate = Claim ProcessId Key Key
    deriving (Show, Typeable)

data PairRequest = Get ProcessId Key 
    | Set ProcessId Key Value
    deriving (Show, Typeable)

data PairReply = RGet Key (Maybe Value)
    | RSet ProcessId
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
    put (RSet pid)    = do putWord8 3; put (pid)
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

setKey :: CacheState -> Key -> Value -> Process ProcessId
setKey state k val = do
    cache <- liftIO $ readMVar state
    let slice = L.find (sliceHandleKey k) $ slices cache
    case slice of
        Nothing -> (liftIO $ modifyMVar state f) >> getSelfPid
                        where f st0 = do let st1 = addPair st0 k val
                                         return (st1, ())
        Just sl -> storeKey k val $ process sl

getKey :: CacheState -> Key -> Process (Maybe Value) 
getKey state k = do
    cache <- liftIO $ readMVar state
    let val = M.lookup k $ pairs cache
    case val of 
        (Just _) -> return val
        Nothing  -> do
            let slice = L.find (sliceHandleKey k) $ slices cache
            case slice of
                Nothing -> return Nothing
                Just sl -> retrieveKey k $ process sl

storeKey :: Key -> Value -> ProcessId -> Process ProcessId
storeKey k val pid = do
    me <- getSelfPid
    send pid $ Set me k val
    msg <- expect -- XXX ordering may be wrong
    case msg of
        RSet keyPid -> return keyPid
        _          -> fail "expecting a RSet" 

retrieveKey :: Key -> ProcessId -> Process (Maybe Value)
retrieveKey k pid = do
    me <- getSelfPid
    send pid $ Get me k
    msg <- expect -- XXX ordering may be wrong
    case msg of
        RGet k val -> return val
        _          -> fail "expecting a RGet"

pairsManager :: CacheState -> Process ()
pairsManager state = do
    forever $ do
        msg <- expect
        case msg of
            Get caller k -> do
                val <- getKey state k
                send caller $ RGet k val
            Set caller k val -> do
                pid <- setKey state k val
                send caller $ RSet pid

sliceManager :: CacheState -> Process ()
sliceManager state = do
    getSelfPid >>= register "slice.server"
    forever $ receiveWait   [ match (remoteSliceDied state) , 
                              match (sliceClaimed state)
                            ]

remoteSliceDied :: CacheState -> ProcessMonitorNotification -> Process ()
remoteSliceDied state (ProcessMonitorNotification _ pid reason) = do
    liftIO $ modifyMVar state f 
            where f st0 = do let st1 = removeAllSlicesForPid st0 pid
                             return (st1, ())

sliceClaimed :: CacheState -> SliceUpdate -> Process ()
sliceClaimed state (Claim pid k0 k1) = do
    monitor pid -- XXX this will setup multiple monitors for same process
    liftIO $ modifyMVar state f 
            where f st0 = do let st1 = addSlice st0 $ Slice pid k0 k1
                             return (st1, ())


sliceMessage :: SliceUpdate -> Process ()
sliceMessage = nsend "slice.server"

remotable ['sliceMessage]
remotables = __remoteTable $ initRemoteTable

forwardSliceUpdate :: SliceUpdate -> NodeId -> Process ()
forwardSliceUpdate msg n = spawn n ($(mkClosure 'sliceMessage) (msg)) >> return ()

runPort :: String -> Key -> Key -> IO ()
runPort port k0 k1 = do 
    cache <- liftIO $ newMVar $ Cache M.empty []
    backend <- initializeBackend "localhost" port remotables
    me <- newLocalNode backend
    runProcess me $ do
        spawnLocal $ sliceManager cache
        pairsMan  <- spawnLocal $ pairsManager cache
        liftIO $ forkProcess me $ claimSliceToOtherNodes (Slice pairsMan k0 k1) backend

        liftIO $ threadDelay 5000000 -- XXX this is an horrible hack to allow piping into stding after an amount of time reasonable enough
        forever $ do 
            str <- liftIO $ getLine
            case (break (== ' ') str) of
                ("keys","")  -> liftIO $ readMVar cache >>= print . M.keys . pairs
                ("cache","") -> liftIO $ readMVar cache >>= print
                ("get",k)    -> getKey cache (read $ tail k) >>= liftIO . print
                ("set",kv)   -> do let (k,v) = break (== ' ') (tail kv)
                                   setKey cache (read k) (tail v) >>= liftIO . print
                _          -> liftIO $ print "not understood"

claimSliceToOtherNodes :: Slice -> Backend -> Process ()
claimSliceToOtherNodes (Slice pid k0 k1) backend = do
    forever $ do
        nodes <- liftIO $ findPeers backend 2000000
        me <- getSelfNode
        forM_ (filter (/= me) nodes) (forwardSliceUpdate (Claim pid k0 k1))

main :: IO ()
main = do
    args <- getArgs
    case args of
        [port] -> runPort port 0 65535
        [port,sk0,sk1] -> runPort port k0 k1
                    where (k0,k1) = (minimum ks, maximum ks)
                                    where ks = map read [sk0,sk1]
