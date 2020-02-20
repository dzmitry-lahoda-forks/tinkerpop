module Gremlin.Net
open Expecto
open Gremlin.Net.Driver
open System.Collections.Generic
open System.Threading.Tasks
open System.Threading
open System.Threading.Tasks
open System

module MasterSlave = 
  type Reply = MailboxProcessor<SlaveMessage> -> unit
  and Action = unit -> Async<unit>
  and MasterMessage = 
    | Start
    | Stop
    | Done of MailboxProcessor<SlaveMessage>
  and SlaveMessage = 
    | Act of Reply * Action
    | Stop
    
  let slaveFactory() = MailboxProcessor<SlaveMessage>(fun inbox -> 
    let rec loop() = async {
      let! message = inbox.Receive()
      match message with 
      | Stop ->
        return ()
      | Act (reply,act) ->
          do! act()
          reply(inbox)
          return! loop()
    }
    loop()
  )

  type MasterState = {inFlightMaximum:int; current:int; slaves:MailboxProcessor<SlaveMessage> list}

  let master slaveCount action = MailboxProcessor<MasterMessage>(fun inbox -> 
    let rec loop(state) = async {
      let! message = inbox.Receive()
      match message with 
      | MasterMessage.Start ->
         if List.length state.slaves < state.inFlightMaximum then
            let slave = slaveFactory()
            inbox.Post(MasterMessage.Start)
            return! loop({state with slaves = List.append state.slaves [slave] } )
         else 
            let doneMessage(s) = inbox.Post(Done s)
            state.slaves |> List.iter (fun x -> x.Start(); x.Post(SlaveMessage.Act (doneMessage, action)))
            return! loop({state with current = List.length state.slaves} )
      | Done s ->
          let doneMessage(s) = inbox.Post(Done s)
          s.Post(SlaveMessage.Act (doneMessage, action))
          return! loop(state)
      | MasterMessage.Stop -> 
        state.slaves |> List.iter (fun x -> x.Post(SlaveMessage.Stop);)
        return ()
      | _ ->
        return! loop(state)
    }

    loop({inFlightMaximum = slaveCount; current = 0; slaves = []})
  )

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
    let queueSize = 100// (config.PoolSize * config.MaxInProcessPerConnection)  / 3
    

    let mutable x = 0
    let queries() = async {           
           trace.Flush()
           fileLog.Flush()
           let ms = Interlocked.Increment(&x)
           printfn "Run %d" (ms)
           try 
            trace.TraceInformation( "Start call {0}", ms)
            let! result = client.SubmitAsync(query, parameters) |> Async.AwaitTask
            trace.TraceInformation( "Got result for call {0}", ms)
            return ()
           with 
             | :? AggregateException as ex -> 
                trace.TraceInformation( "Error(from aggregate) {0} {1}", ms, ex.InnerException.ToString())
                return ()    
             | :? NullReferenceException as ex -> 
                trace.TraceInformation( "Error(from NullReferenceException) {0} {1}", ms, ex.InnerException.ToString())
             | ex -> 
                trace.TraceInformation( "Error {0} {1}", ms, ex.InnerException.ToString())
                return ()
        }
    let master = MasterSlave.master queueSize queries    
    master.Start()
    master.Post(MasterSlave.MasterMessage.Start)
    Console.ReadLine()
    0
