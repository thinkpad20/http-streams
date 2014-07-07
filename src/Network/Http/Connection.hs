--
-- HTTP client for use with io-streams
--
-- Copyright © 2012-2013 Operational Dynamics Consulting, Pty Ltd
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DoAndIfThenElse    #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE LambdaCase         #-}

module Network.Http.Connection (
    Connection(..),
        -- constructors only for testing
    makeConnection,
    withConnection,
    openConnection,
    openConnectionSSL,
    closeConnection,
    getHostname,
    getRequestHeaders,
    getHeadersFull,
    sendRequest,
    receiveResponse,
    receiveResponseRaw,
    UnexpectedCompression,
    emptyBody,
    fileBody,
    inputStreamBody,
    debugHandler,
    concatHandler
) where

import Blaze.ByteString.Builder (Builder)
import qualified Blaze.ByteString.Builder as Bld ( flush, fromByteString
                                                 , toByteString)
import qualified Blaze.ByteString.Builder.HTTP as Bld ( chunkedTransferEncoding
                                                    , chunkedTransferTerminator)
import Control.Applicative ((<$>), (<*))
import Control.Exception (bracket)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import Data.Monoid (mempty, (<>))
import Network.Socket ( AddrInfo(..), AddrInfoFlag(..), SocketType(..)
                      , getAddrInfo, defaultProtocol, socket, connect
                      , defaultHints, close)
import OpenSSL (withOpenSSL)
import OpenSSL.Session (SSLContext)
import qualified OpenSSL.Session as SSL
import System.IO.Streams (InputStream, OutputStream, stdout)
import qualified System.IO.Streams as Streams
import qualified System.IO.Streams.SSL as Streams hiding (connect)

import Network.Http.Internal ( Request(..), Response(..), Hostname, Port
                             , EntityBody(..), ExpectMode(..), Headers
                             , ContentEncoding(..), updateHeader
                             , retrieveHeaders, getStatusCode
                             , composeResponseBytes, composeRequestBytes)
import Network.Http.ResponseParser ( UnexpectedCompression
                                   , readResponseHeader, readResponseBody)

--
-- | A connection to a web server.
--
data Connection = Connection 
  { cHost  :: ByteString -- ^ used as the Host: header in the HTTP request.
  , cClose :: IO ()      -- ^ called when the connection should be closed.
  , cIn    :: InputStream ByteString
  , cOut   :: OutputStream Builder
  }

instance Show Connection where
  show c = {-# SCC "Connection.show" #-}
    concat ["Host: ", S.unpack $ cHost c, "\n"]

--
-- | Create a raw Connection object from the given parts. This is
-- primarily of use when testing, for example:
--
-- > fakeConnection :: IO Connection
-- > fakeConnection = do
-- >     o  <- Streams.nullOutput
-- >     i  <- Streams.nullInput
-- >     c  <- makeConnection "www.example.com" (return()) o i
-- >     return c
--
-- is an idiom we use frequently in testing and benchmarking, usually
-- replacing the InputStream with something like:
--
-- >     x' <- S.readFile "properly-formatted-response.txt"
-- >     i  <- Streams.fromByteString x'
--
-- If you're going to do that, keep in mind that you /must/ have CR-LF
-- pairs after each header line and between the header and body to
-- be compliant with the HTTP protocol; otherwise, parsers will
-- reject your message.
--
makeConnection
  :: ByteString
  -- ^ will be used as the @Host:@ header in the HTTP request.
  -> IO ()
  -- ^ an action to be called when the connection is terminated.
  -> OutputStream ByteString
  -- ^ write end of the HTTP client-server connection.
  -> InputStream ByteString
  -- ^ read end of the HTTP client-server connection.
  -> IO Connection
makeConnection host action out inp = 
  Connection host action inp <$> Streams.builderStream out

--
-- | Given an @IO@ action producing a @Connection@, and a computation
-- that needs one, runs the computation, cleaning up the
-- @Connection@ afterwards.
--
-- >     x <- withConnection (openConnection "s3.example.com" 80) $ (\c -> do
-- >         q <- buildRequest $ do
-- >             http GET "/bucket42/object/149"
-- >         sendRequest c q emptyBody
-- >         ...
-- >         return "blah")
--
-- which can make the code making an HTTP request a lot more
-- straight-forward.
--
-- Wraps @bracket@ from @Control.Exception@.
--
withConnection :: IO Connection -> (Connection -> IO γ) -> IO γ
withConnection = flip bracket closeConnection

--
-- | In order to make a request you first establish the TCP
-- connection to the server over which to send it.
--
-- Ordinarily you would supply the host part of the URL here and it will
-- be used as the value of the HTTP 1.1 @Host:@ field. However, you can
-- specify any server name or IP address and set the @Host:@ value
-- later with 'Network.Http.Client.setHostname' when building the
-- request.
--
-- Usage is as follows:
--
-- >     c <- openConnection "localhost" 80
-- >     ...
-- >     closeConnection c
--
-- More likely, you'll use 'withConnection' to wrap the call in order
-- to ensure finalization.
--
-- HTTP pipelining is supported; you can reuse the connection to a
-- web server, but it's up to you to ensure you match the number of
-- requests sent to the number of responses read, and to process those
-- responses in order. This is all assuming that the /server/ supports
-- pipelining; be warned that not all do. Web browsers go to
-- extraordinary lengths to probe this; you probably only want to do
-- pipelining under controlled conditions. Otherwise just open a new
-- connection for subsequent requests.
--
openConnection :: Hostname -> Port -> IO Connection
openConnection host port = do
  let hints = defaultHints {addrFlags = [AI_ADDRCONFIG, AI_NUMERICSERV]}
  addrs <- getAddrInfo (Just hints) (Just $ S.unpack host) (Just $ show port)
  case addrs of
    [] -> error $ "Couldn't get address info for " <> show host
    AddrInfo{..}:_ -> do
      sock <- socket addrFamily Stream defaultProtocol
      connect sock addrAddress

      -- Create the input and output socket streams from the socket
      (i, o') <- Streams.socketToStreams sock
      o <- Streams.builderStream o'

      let host' = if port == 80 then host else host <> ":" <> S.pack (show port)
      return $! Connection host' (close sock) i o

--
-- | Open a secure connection to a web server.
--
-- > import OpenSSL (withOpenSSL)
-- >
-- > main :: IO ()
-- > main = do
-- >     ctx <- baselineContextSSL
-- >     c <- openConnectionSSL ctx "api.github.com" 443
-- >     ...
-- >     closeConnection c
--
-- If you want to tune the parameters used in making SSL connections,
-- manually specify certificates, etc, then setup your own context:
--
-- > import OpenSSL.Session (SSLContext)
-- > import qualified OpenSSL.Session as SSL
-- >
-- >     ...
-- >     ctx <- SSL.context
-- >     ...
--
-- See "OpenSSL.Session".
--
-- Crypto is as provided by the system @openssl@ library, as wrapped
-- by the @HsOpenSSL@ package and @openssl-streams@.
--
-- /There is no longer a need to call @withOpenSSL@ explicitly; the
-- initialization is invoked once per process for you./
--
openConnectionSSL :: SSLContext -> Hostname -> Port -> IO Connection
openConnectionSSL ctx host port = withOpenSSL $ do
  addrs <- getAddrInfo Nothing (Just $ S.unpack host) (Just $ show port)
  case addrs of
    [] -> error $ "Couldn't get address of host " <> show host
    AddrInfo{..}:_ -> do
      sock <- socket addrFamily Stream defaultProtocol
      connect sock addrAddress
      ssl <- SSL.connection ctx sock
      SSL.connect ssl
      (i, o') <- Streams.sslToStreams ssl
      o <- Streams.builderStream o'
      let h = if port == 443 then host else host <> ":" <> S.pack (show port)
          closeSSL = SSL.shutdown ssl SSL.Unidirectional >> close sock
      return $! Connection h closeSSL i o


--
-- | Having composed a 'Request' object with the headers and metadata for
-- this connection, you can now send the request to the server, along
-- with the entity body, if there is one. For the rather common case of
-- HTTP requests like 'GET' that don't send data, use 'emptyBody' as the
-- output stream:
--
-- >     sendRequest c q emptyBody
--
-- For 'PUT' and 'POST' requests, you can use 'fileBody' or
-- 'inputStreamBody' to send content to the server, or you can work with
-- the @io-streams@ API directly:
--
-- >     sendRequest c q (\o ->
-- >         Streams.write (Just (Builder.fromString "Hello World\n")) o)
--
{-
    I would like to enforce the constraints on the Empty and Static
    cases shown here, but those functions take OutputStream ByteString,
    and we are of course working in OutputStream Builder by that point.
-}
sendRequest :: Connection -> Request -> (OutputStream Builder -> IO α) -> IO α
sendRequest Connection{..} req@(Request{..}) handler = do
  -- Write the headers.
  Streams.write (Just $ composeRequestBytes req cHost) cOut

  -- Deal with the expect-continue mess.
  body <- case qExpect of
    Normal -> return qBody
    Continue -> do
      Streams.write (Just Bld.flush) cOut
      readResponseHeader cIn >>= \r -> case getStatusCode r of
        -- 100 means ok to send
        100 -> return qBody
        -- Otherwise, put the response back
        _   -> do 
          Streams.unRead (Bld.toByteString $ composeResponseBytes r) cIn
          return Empty

  -- Write the body, if there is one.
  result <- case body of
    Empty    -> handler =<< Streams.nullOutput
    Chunking -> do
      res <- handler =<< Streams.contramap Bld.chunkedTransferEncoding cOut
      Streams.write (Just Bld.chunkedTransferTerminator) cOut
      return res
    Static _ -> handler cOut

  -- Push the stream out by flushing the output buffers.
  Streams.write (Just Bld.flush) cOut
  return result

--
-- | Get the virtual hostname that will be used as the @Host:@ header in
-- the HTTP 1.1 request. Per RFC 2616 § 14.23, this will be of the form
-- @hostname:port@ if the port number is other than the default, ie 80
-- for HTTP.
--
getHostname :: Connection -> Request -> ByteString
getHostname Connection{..} Request{..} = maybe cHost id qHost

{-# DEPRECATED getRequestHeaders "use retrieveHeaders . getHeadersFull instead" #-}
getRequestHeaders :: Connection -> Request -> [(ByteString, ByteString)]
getRequestHeaders c r = ("Host", getHostname c r) : retrieveHeaders (qHeaders r)

--
-- | Get the headers that will be sent with this request. You likely won't
-- need this but there are some corner cases where people need to make
-- calculations based on all the headers before they go out over the wire.
--
-- If you'd like the request headers as an association list, import the header
-- functions:
--
-- > import Network.Http.Types
--
-- then use @Network.Http.Types.retrieveHeaders@ as follows:
--
-- >>> let kvs = retrieveHeaders $ getHeadersFull c q
-- >>> :t kvs
-- :: [(ByteString, ByteString)]
--
getHeadersFull :: Connection -> Request -> Headers
getHeadersFull c req = updateHeader (qHeaders req) "Host" (getHostname c req)

--
-- | Handle the response coming back from the server. This function
-- hands control to a handler function you supply, passing you the
-- @Response@ object with the response headers and an @InputStream@
-- containing the entity body.
--
-- For example, if you just wanted to print the first chunk of the
-- content from the server:
--
-- >     receiveResponse c (\p i -> do
-- >         m <- Streams.read i
-- >         case m of
-- >             Just bytes -> putStr bytes
-- >             Nothing    -> return ())
--
-- Obviously, you can do more sophisticated things with the
-- @InputStream@, which is the whole point of having an @io-streams@
-- based HTTP client library.
--
-- The final value from the handler function is the return value of
-- @receiveResponse@, if you need it.
--
-- Throws @UnexpectedCompression@ if it doesn't know how to handle the
-- compression format used in the response.
--
{-
    The reponse body coming from the server MUST be fully read, even
    if (especially if) the users's handler doesn't consume it all.
    This is necessary to maintain the HTTP protocol invariants;
    otherwise pipelining would not work. It's not entirely clear
    *which* InputStream is being drained here; the underlying
    InputStream ByteString in Connection remains unconsumed beyond the
    threshold of the current response, which is exactly what we need.
-}
receiveResponse :: Connection 
                -> (Response -> InputStream ByteString -> IO β) -> IO β
receiveResponse Connection{..} handler = do
  p  <- readResponseHeader cIn
  inp <- readResponseBody p cIn
  handler p inp <* Streams.skipToEof inp

--
-- | This is a specialized variant of 'receiveResponse' that /explicitly/ does
-- not handle the content encoding of the response body stream (it will not
-- decompress anything). Unless you really want the raw gzipped content coming
-- down from the server, use @receiveResponse@.
--
{-
    See notes at receiveResponse.
-}
receiveResponseRaw :: Connection 
                   -> (Response -> InputStream ByteString -> IO β) -> IO β
receiveResponseRaw Connection{..} handler = do
  p  <- readResponseHeader cIn
  i' <- readResponseBody (p {pContentEncoding = Identity}) cIn
  handler p i' <* Streams.skipToEof i'

--
-- | Use this for the common case of the HTTP methods that only send
-- headers and which have no entity body, i.e. 'GET' requests.
--
emptyBody :: OutputStream Builder -> IO ()
emptyBody _ = return ()

--
-- | Specify a local file to be sent to the server as the body of the
-- request.
--
-- You use this partially applied:
--
-- >     sendRequest c q (fileBody "/etc/passwd")
--
-- Note that the type of @(fileBody \"\/path\/to\/file\")@ is just what
-- you need for the third argument to 'sendRequest', namely
--
-- >>> :t filePath "hello.txt"
-- :: OutputStream Builder -> IO ()
--
{-
    Relies on Streams.withFileAsInput generating (very) large chunks [which it
    does]. A more efficient way to do this would be interesting.
-}
fileBody :: FilePath -> OutputStream Builder -> IO ()
fileBody path out = Streams.withFileAsInput path $ flip inputStreamBody out

--
-- | Read from a pre-existing 'InputStream' and pipe that through to the
-- connection to the server. This is useful in the general case where
-- something else has handed you stream to read from and you want to use
-- it as the entity body for the request.
--
-- You use this partially applied:
--
-- >     i <- getStreamFromVault                    -- magic, clearly
-- >     sendRequest c q (inputStreamBody i)
--
-- This function maps "Bld.fromByteString" over the input, which will
-- be efficient if the ByteString chunks are large.
--
{-
    Note that this has to be 'supply' and not 'connect' as we do not
    want the end of stream to prematurely terminate the chunked encoding
    pipeline!
-}
inputStreamBody :: InputStream ByteString -> OutputStream Builder -> IO ()
inputStreamBody inp out = 
  Streams.map Bld.fromByteString inp >>= flip Streams.supply out


--
-- | Print the response headers and response body to @stdout@. You can
-- use this with 'receiveResponse' or one of the convenience functions
-- when testing. For example, doing:
--
-- >     c <- openConnection "kernel.operationaldynamics.com" 58080
-- >
-- >     q <- buildRequest $ do
-- >         http GET "/time"
-- >
-- >     sendRequest c q emptyBody
-- >
-- >     receiveResponse c debugHandler
--
-- would print out:
--
-- > HTTP/1.1 200 OK
-- > Transfer-Encoding: chunked
-- > Content-Type: text/plain
-- > Vary: Accept-Encoding
-- > Server: Snap/0.9.2.4
-- > Content-Encoding: gzip
-- > Date: Mon, 21 Jan 2013 06:13:37 GMT
-- >
-- > Mon 21 Jan 13, 06:13:37.303Z
--
-- or thereabouts.
--
debugHandler :: Response -> InputStream ByteString -> IO ()
debugHandler resp inp = do
  let prepare = S.filter (/= '\r') . Bld.toByteString . composeResponseBytes
  S.putStr $ prepare resp
  Streams.connect inp stdout

--
-- | Sometimes you just want the entire response body as a single blob.
-- This function concatenates all the bytes from the response into a
-- ByteString. If using the main @http-streams@ API, you would use it
-- as follows:
--
-- >    ...
-- >    x' <- receiveResponse c concatHandler
-- >    ...
--
-- The methods in the convenience API all take a function to handle the
-- response; this function is passed directly to the 'receiveResponse'
-- call underlying the request. Thus this utility function can be used
-- for 'get' as well:
--
-- >    x' <- get "http://www.example.com/document.txt" concatHandler
--
-- Either way, the usual caveats about allocating a single object from 
-- streaming I/O apply: do not use this if you are not absolutely certain that
-- the response body will fit in a reasonable amount of memory.
--
-- Note that this function makes no discrimination based on the
-- response's HTTP status code. You're almost certainly better off
-- writing your own handler function.
--
{-
    I'd welcome a better name for this function.
-}
concatHandler :: Response -> InputStream ByteString -> IO ByteString
concatHandler _ inp = do
  inp' <- Streams.map Bld.fromByteString inp
  Bld.toByteString <$> Streams.fold (<>) mempty inp'

--
-- | Shutdown the connection. You need to call this release the
-- underlying socket file descriptor and related network resources. To
-- do so reliably, use this in conjunction with @openConnection@ in a
-- call to @Control.Exception.bracket@. Or just use
-- @withConnection@ does):
--
-- > --
-- > -- Make connection, cleaning up afterward
-- > --
-- >
-- > foo :: IO ByteString
-- > foo = bracket
-- >    (openConnection "localhost" 80)
-- >    (closeConnection)
-- >    (doStuff)
-- >
-- > --
-- > -- Actually use Connection to send Request and receive Response
-- > --
-- >
-- > doStuff :: Connection -> IO ByteString
--
-- or, just use @withConnection@.
--
-- While returning a ByteString is probably the most common use case,
-- you could conceivably do more processing of the response in 'doStuff'
-- and have it and 'foo' return a different type.
--
closeConnection :: Connection -> IO ()
closeConnection c = cClose c
