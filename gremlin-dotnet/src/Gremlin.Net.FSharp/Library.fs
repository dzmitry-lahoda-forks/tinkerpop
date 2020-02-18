namespace Gremlin.Net.Driver

open System
open System.Net.WebSockets
open System.Diagnostics
open Gremlin.Net.Driver
open Gremlin.Net.Structure.IO.GraphSON
open FSharpx
open FSharp.Control.Tasks.V2

module Result =

    let tap (f: 'ok -> unit) (result: Result<'ok, 'error>) =
        match result with
        | Ok ok ->
            f ok
            Ok ok
        | Error error -> Error error

[<CLIMutable>]
type CosmosGraphConfig = {
    ConnectionString: string
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
                ex -> tryMakeClient collectionLink authKey rest
        | [] ->
            Error "Can't connect to any of the regions"

    let createClient () =
        let collectionLink = sprintf "/dbs/%s/colls/%s" config.Database config.Collection
        let authKey = config.AuthKey
        let locations = config.Locations |> List.ofArray
        match tryMakeClient collectionLink authKey locations with
        | Ok gremlinClient -> gremlinClient
        | Error error -> failwith error

    let mutable client = createClient ()

    let clientLock = Object()

    member this.Recreate(failedClient: GremlinClient) =
        lock clientLock <| fun () ->
            if Object.ReferenceEquals(failedClient, this.Client) then
                logger.TraceInformation("Creating new client")
                (this.Client :> IDisposable).Dispose()
                client <- createClient()

    member __.Client
        with get() = lock clientLock (fun () -> client)

    interface IGremlinClient with
        member this.SubmitAsync(requestMessage) = task {
                let client = this.Client
                try
                    return! client.SubmitAsync(requestMessage) |> Async.AwaitTask
                with
                | :? WebSocketException as ex ->
                    logger.TraceInformation("Got a WebSocketException {0}. Will try to recreate agent", ex)
                    this.Recreate(client)
                    return! this.Client.SubmitAsync(requestMessage) |> Async.AwaitTask
                | ex when (ex.InnerException :? WebSocketException) ->
                    logger.TraceInformation("Got a WebSocketException {0}. Will try to recreate agent", ex.InnerException)
                    this.Recreate(client)
                    return! this.Client.SubmitAsync(requestMessage) |> Async.AwaitTask
            }

        member __.Dispose() =
            client.Dispose()