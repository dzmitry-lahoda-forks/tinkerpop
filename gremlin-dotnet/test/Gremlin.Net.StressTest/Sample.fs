module Tests

open Expecto
open Gremlin.Net.Driver

type Serialisers() =

  [<Benchmark>]
  member __.FastSerialiserAlt() = ()

  [<Benchmark>]
  member __.SlowSerialiser() = ()

  [<Benchmark(Baseline = true)>]
  member __.FastSerialiser() = ()

[<Tests>]
let tests =
  testList "performance tests" [
    test "three serialisers" {
      // TODO: run N Async threads whic do add or updarte node with empty guid id and no properties
      benchmark<Serialisers> benchmarkConfig (fun _ -> null) |> ignore
    }
  ]