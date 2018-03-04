{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}


module Main
    ( parse
    , parseAndResolve
    , catalog
    , main
    ) where

import Database.Sql.Type hiding (catalog)

import           Database.Sql.Util.Scope (runResolverWarn)
import qualified Database.Sql.Vertica.Parser as VP
import           Database.Sql.Vertica.Type (VerticaStatement, resolveVerticaStatement, Vertica)

import Database.Sql.Util.Tables
import Database.Sql.Util.Columns
import Database.Sql.Util.Joins
import Database.Sql.Util.Lineage.Table

import           Data.Either
import           Data.Functor (void)
import qualified Data.HashMap.Strict as HMS
import qualified Data.List as L
import qualified Data.Map as M
import           Data.Proxy
import qualified Data.Set as S
import qualified Data.Text.Lazy as TL

--import Text.PrettyPrint

import Data.Text.Lazy.Encoding (decodeUtf8)
import Web.Scotty hiding (text)
import qualified Web.Scotty as WS
import Control.Monad.Trans
import Data.Monoid ((<>))
import Network.Wai.Middleware.RequestLogger
import GHC.Generics
import Data.Aeson

-- let's provide a really simple function to do parsing!
-- It will have ungraceful error handling.
parse :: TL.Text -> VerticaStatement RawNames ()
parse sql = case void <$> VP.parse sql of
    Right q -> q
    Left err -> error $ show err

-- and construct a catalog, with tables `foo` (columns a, b, and c) and `bar` (columns x, y, and z)
catalog :: Catalog
catalog = makeDefaultingCatalog catalogMap [defaultSchema] defaultDatabase
  where
    defaultDatabase :: DatabaseName ()
    defaultDatabase = DatabaseName () "defaultDatabase"

    defaultSchema :: UQSchemaName ()
    defaultSchema = mkNormalSchema "public" ()

    foo :: (UQTableName (), SchemaMember)
    foo = ( QTableName () None "foo", persistentTable [ QColumnName () None "a"
                                                      , QColumnName () None "b"
                                                      , QColumnName () None "c"
                                                      ] )

    bar :: (UQTableName (), SchemaMember)
    bar = ( QTableName () None "bar", persistentTable [ QColumnName () None "x"
                                                      , QColumnName () None "y"
                                                      , QColumnName () None "z"
                                                      ] )

    catalogMap :: CatalogMap
    catalogMap = HMS.singleton defaultDatabase $
                     HMS.fromList [ ( defaultSchema, HMS.fromList [ foo , bar ] ) ]

-- let's provide a really simple function that combines parsing + resolving.
-- We'll hardcode the catalog and leave the error handling ungraceful, still.
parseAndResolve :: TL.Text -> (VerticaStatement ResolvedNames (), [ResolutionError ()])
parseAndResolve sql = case runResolverWarn (resolveVerticaStatement $ parse sql) (Proxy :: Proxy Vertica) catalog of
    (Right queryResolved, resolutions) -> (queryResolved, lefts resolutions)
    (Left err, _) -> error $ show err


-- demoTableLineage :: TL.Text -> Doc
-- demoTableLineage sql = draw $ getTableLineage $ fst $ parseAndResolve sql
--   where
--     draw :: M.Map FQTN (S.Set FQTN) -> Doc
--     draw xs = case M.assocs xs of
--                   [] -> text "no tables modified"
--                   xs' -> vcat $ map drawAssoc xs'

--     drawAssoc :: (FQTN, S.Set FQTN) -> Doc
--     drawAssoc (tgt, srcs) = case S.toList srcs of
--                                 [] -> hsep [drawFQTN tgt, text "no longer has data"]
--                                 srcs' -> hsep [ drawFQTN tgt
--                                               , text "after the query depends on"
--                                               , drawDeps srcs'
--                                               , text "before the query"
--                                               ]


--     drawDeps :: [FQTN] -> Doc
--     drawDeps srcs = hcat $ L.intersperse ", " $ map drawFQTN srcs

-- pretty printing helpers
drawFQTN :: FullyQualifiedTableName -> TL.Text
drawFQTN FullyQualifiedTableName{..} =  TL.intercalate "." [fqtnSchemaName, fqtnTableName]

drawFQCN :: FullyQualifiedColumnName -> TL.Text
drawFQCN FullyQualifiedColumnName{..} = TL.intercalate "." [fqcnSchemaName, fqcnTableName, fqcnColumnName]

drawField :: (FullyQualifiedColumnName, [StructFieldName ()]) -> TL.Text
drawField (fqcn, fields) = foldl1 combineWithDot (drawFQCN fqcn : map drawStructFieldName fields)
  where
    combineWithDot x y = x <> "." <> y
    drawStructFieldName (StructFieldName _ name) = name

main :: IO ()
main = scotty 3000 $ do
  middleware logStdoutDev
  post "/explain" $ do
    b <- body
    WS.json $ buildQueryToo $ decodeUtf8 b 

data QueryToo = QueryToo {
    joins :: [[TL.Text]]
    , columns :: [[TL.Text]]
    , tables :: [TL.Text]
} deriving (Generic, Show)

instance ToJSON QueryToo where
  toEncoding = genericToEncoding defaultOptions

instance FromJSON QueryToo

buildQueryToo :: TL.Text -> QueryToo
buildQueryToo sql = QueryToo {
  tables = getTablesToo sql
  , joins = getJoinsToo sql
  , columns = getColumnsToo sql
}

getTablesToo :: TL.Text -> [TL.Text]
getTablesToo sql = draw $ getTables $ fst $ parseAndResolve sql
  where
    draw :: S.Set FullyQualifiedTableName -> [TL.Text]
    draw xs = case S.toList xs of
                  [] -> [TL.empty]
                  xs' -> map drawFQTN xs'

getColumnsToo :: TL.Text -> [[TL.Text]]
getColumnsToo sql = draw $ getColumns $ fst $ parseAndResolve sql
  where
    draw :: S.Set (FullyQualifiedColumnName, Clause) -> [[TL.Text]]
    draw xs = case S.toList xs of
                  [] -> [[TL.empty]]
                  xs' -> map drawCol xs'

    drawCol :: (FullyQualifiedColumnName, Clause) -> [TL.Text]
    drawCol (col, clause) = [drawFQCN col, clause]

getJoinsToo :: TL.Text -> [[TL.Text]]
getJoinsToo sql = draw $ getJoins $ fst $ parseAndResolve sql
  where
    draw :: S.Set ((FullyQualifiedColumnName, [StructFieldName ()]), (FullyQualifiedColumnName, [StructFieldName ()])) -> [[TL.Text]]
    draw xs = case S.toList xs of
                  [] -> [[TL.empty]]
                  xs' -> map drawJoin xs'

    drawJoin :: ((FullyQualifiedColumnName, [StructFieldName ()]), (FullyQualifiedColumnName, [StructFieldName ()])) -> [TL.Text]
    drawJoin (f1, f2) = [drawField f1, drawField f2]
