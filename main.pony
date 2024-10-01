use "collections"

actor ConvergenceDetector
  let _env: Env

  new create(env: Env) =>
    _env = env

  be report() =>
    _env.out.print("Timer received a message.")

actor Worker
  var _sum: F64 = 0
  var _weight: F64 = 1.0
  var _rumourCount: I64 = 0
  var _neighbors: Array[Worker tag]
  var _supervisor: ConvergenceDetector
  var _id: I64
  let _env: Env
  var _converged: Bool = false

  new create(env: Env, supervisor: ConvergenceDetector, id: I64) =>
    _supervisor = supervisor
    _id = id
    _neighbors = []
    _env = env
    _env.out.print("Worker " + id.string() + " created.")

  be countNeighbors(main: Main) =>
    main.receive_neighbor_count(_id, _neighbors.size())

  be notify_ready(main: Main) =>
    main.worker_ready(this)

  be assignNeighbors(neighbor: Worker tag) =>
    _neighbors.push(neighbor)

actor Main
  var _supervisor: ConvergenceDetector
  var _workers: Array[Worker tag] = Array[Worker tag]
  var _totalNodes: I64 = 0
  let _env: Env
  var _workers_ready: USize = 0

  new create(env: Env) =>
    _env = env
    _supervisor = ConvergenceDetector(env)

    try
      _totalNodes = _env.args(1)?.i64()?
      let topology = _env.args(2)?
      let algorithm = _env.args(3)?

      if topology == "line" then
        createWorkers()
        _env.out.print("Number of workers created: " + _workers.size().string()) // Check array size
        _env.out.print("Total nodes: " + _totalNodes.string())
        _env.out.print("Topology: " + topology.string())
        _env.out.print("Algorithm: " + algorithm.string())
        _env.out.print("Number of workers: " + _workers.size().string())
      else
        _env.out.print("Only 'line' topology is supported for now.")
      end
    else
      _env.out.print("Invalid input.")
    end

  be worker_ready(worker: Worker) =>
    _workers_ready = _workers_ready + 1
    if _workers_ready == _totalNodes.usize() then
      initializeNeighbors()
    end

  // Create Worker instances
  fun ref createWorkers() =>
    for i in Range[I64](0, _totalNodes) do
      let worker = Worker(_env, _supervisor, i)
      _workers.push(worker)
      worker.notify_ready(this)
    end


  fun ref initializeNeighbors() =>
    var neighbors = Array[Worker tag]

    try
      for i in Range[I64](0, _totalNodes) do
        _env.out.print("Initializing neighbors for Worker " + i.string())

        var current: Worker tag = _workers(i.usize())?

        if i > 0 then
          current.assignNeighbors(_workers((i - 1).usize())?) // Left neighbor
        end

        if i < (_totalNodes - 1) then
          current.assignNeighbors(_workers((i + 1).usize())?) // Right neighbor
        end

        current.countNeighbors(this)

      end
    else
      _env.out.print("Error assigning neighbors")
    end

  be receive_neighbor_count(worker_id: I64, count: USize) =>
    _env.out.print("Worker " + worker_id.string() + " has " + count.string() + " neighbors")
