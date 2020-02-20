module Gremlin.Net
open Expecto
open Gremlin.Net.Driver
open System.Collections.Generic
open System.Threading.Tasks
open System.Threading
open System.Threading.Tasks
open System

[<EntryPoint>]
let main argv =
    let config = {
        PoolSize = 25;
        MaxInProcessPerConnection = 25;

    }
    let trace = Diagnostics.TraceSource("Gremlin")
    trace.Switch.Level <- Diagnostics.SourceLevels.All
    let console = new Diagnostics.ConsoleTraceListener()
    use file = System.IO.File.OpenWrite("Gremlin.Net.StressTest.log")
    let fileLog = new Diagnostics.TextWriterTraceListener(file)
    trace.Listeners.Add(console) |> ignore
    trace.Listeners.Add(fileLog) |> ignore
    use client = new RenewableGremlinClient(config, trace)
    let guid = "f6fdca50-cb42-499d-893b-f8d2bdc83c74" :> obj
    let query = 
        sprintf 
            """
             g.V().hasId(id)
                .fold()
                .coalesce(
                    unfold(),
                    addV(id)
                        .property(id, id)
                        .property('pk', id))
            """

    let parameters = [("id", guid)] |> Map |> Dictionary
    
    client.SubmitAsync(query, parameters) |> Async.AwaitTask |> Async.RunSynchronously
    printfn "Start clumsy"
    Console.ReadLine() |> ignore
    let queueSize = (config.PoolSize * config.MaxInProcessPerConnection)  / 4
    let mutable current = 0
    // TODO: better model requesters as actors which return results into acotr and antoher actor which check that pool is free and sends messages to them
    let increment() = 
        let my = Volatile.Read(&current)
        if my + 1 >= queueSize then 
            false
        else            
            let previous = Interlocked.CompareExchange(&current, my + 1, my)
            previous = my
    let queries = 
        seq { 1 .. 5000_000} 
        |> Seq.map (fun (ms) -> async {           
           while increment() |> not do
             do! Async.Sleep 10
           trace.Flush()
           fileLog.Flush()
           printfn "Run %d" ms
           try 
            let! result = client.SubmitAsync(query, parameters) |> Async.AwaitTask
            Interlocked.Decrement(&current)
            return result |> Result.Ok
           with 
             | :? AggregateException as ex -> 
                Interlocked.Decrement(&current)
                trace.TraceInformation( "Error(from aggregate) {0} {1}", ms, ex.InnerException.ToString())
                return ex.InnerException |> Result.Error       
             | :? NullReferenceException as ex -> 
                Interlocked.Decrement(&current)
                trace.TraceInformation( "Error(from NullReferenceException) {0} {1}", ms, ex.InnerException.ToString())
                return ex.InnerException |> Result.Error                     
             | ex -> 
                Interlocked.Decrement(&current)
                trace.TraceInformation( "Error {0} {1}", ms, ex.InnerException.ToString())
                return ex |> Result.Error   
        }
        )
        |> Async.Parallel
        
    let result =  queries |> Async.RunSynchronously 
    printfn "%A" result
    0
