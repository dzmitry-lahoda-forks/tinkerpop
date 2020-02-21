namespace Gremlin.Net.Driver

open System
open System.Net.WebSockets
open System.Diagnostics
open Gremlin.Net.Driver
open Gremlin.Net.Structure.IO.GraphSON
open FSharpx
open FSharp.Control.Tasks.V2
open System.Diagnostics
open System.Diagnostics.Tracing
open Gremlin.Net.Driver.Exceptions
open Gremlin.Net.Driver.Messages

module Result =

    let tap (f: 'ok -> unit) (result: Result<'ok, 'error>) =
        match result with
        | Ok ok ->
            f ok
            Ok ok
        | Error error -> Error error

[<CLIMutable>]
type CosmosGraphConfig = {
    PoolSize: Int32
    MaxInProcessPerConnection: Int32
    AuthKey: string
    Locations: string array
    Database: string
    Collection: string
}

type RenewableGremlinClient(config: CosmosGraphConfig, logger:TraceSource) =
    let makeClient server =
        let settings =
            ConnectionPoolSettings(
                MaxInProcessPerConnection = config.MaxInProcessPerConnection,
                PoolSize = config.PoolSize
            )    
        new GremlinClient(server, GraphSON2Reader(), GraphSON2Writer(), GremlinClient.GraphSON2MimeType, settings)

    let tryParseLocation (location: string) =
        match String.splitString [|":"|] StringSplitOptions.RemoveEmptyEntries location with
        | [|host ; port |] ->
            match Int32.TryParse port with
            | true, correctPort -> Ok (host, correctPort)
            | false, _ -> sprintf "Invalid format of port: %s" port |> Error
        | _ -> sprintf "Invalid format of location: %s" location |> Error

    let rec tryMakeClient collectionLink authKey : string list -> Result<GremlinClient, string> =
        function
        | location::rest ->
            try
                tryParseLocation location
                |> Result.tap (fun _ -> logger.TraceInformation("Trying to connect to Gremlin location {0}", location))
                |> Result.map (fun (host, port) -> GremlinServer(host, port, true, collectionLink, authKey))
                |> Result.map makeClient
                |> Result.tap (fun _ -> logger.TraceInformation("Successfully connected to Gremlin location {0}", location))
            with
                newEx -> 
                    printfn "%A" newEx
                    tryMakeClient collectionLink authKey rest
        | [] ->
            Error "Region"

    let createClient () =
        let collectionLink = sprintf "/dbs/%s/colls/%s" config.Database config.Collection
        let authKey = config.AuthKey
        let locations = config.Locations |> List.ofArray
        match tryMakeClient collectionLink authKey locations with
        | Ok gremlinClient -> gremlinClient
        | Error error -> failwith error

    let mutable client = createClient ()


    interface IGremlinClient with
        member this.SubmitAsync(requestMessage) = task {
                try
                    return! client.SubmitAsync(requestMessage)
                with              
                | ex ->
                    // next issues can happend for normal call in flight call was bad and we hit into closed-closing connection
                    let error = if ex :? AggregateException then ex.InnerException else ex
                    let code = 
                        match error with 
                        |  :? ResponseException as re -> 
                            re.StatusCode = ResponseStatusCode.ServerError || re.StatusCode = ResponseStatusCode.ServerTimeout
                        | _ -> false
                    let reSubmit =  
                        error :? WebSocketException // usual network errors
                        || error :? ConnectionPoolBusyException // can happend even not in case of full exostion
                        || error :? ServerUnavailableException // same as above, see code of client
                        || error :? InvalidOperationException // for bugs and corrupted network and some other in flight call lead to error + close https://issues.apache.org/jira/browse/TINKERPOP-2019 https://github.com/apache/tinkerpop/pull/1250s
                        || code
                    if reSubmit then    
                        logger.TraceInformation("Got a WebSocketException {0}. Will try to recreate agent", ex)
                        // NOTE: we already have wait here becuse of socket client recreation
                        return! client.SubmitAsync(requestMessage)
                    else
                        logger.TraceInformation("Not handled error {0}", ex)
                        reraise' ex
                        return null
            }

        member __.Dispose() =
            client.Dispose()