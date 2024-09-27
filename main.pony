use "collections"

//use @rand[I32]()
//use @sqrt[F64](x: F64)

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
  var _neighbors: Array[Worker]
  var _supervisor: ConvergenceDetector
  var _id: I64
  let _env: Env
  var _converged: Bool = false

  new create(supervisor: ConvergenceDetector, id: I64, env: Env) =>
    _supervisor = supervisor
    _id = id
    _neighbors = []
    _env = env
    _env.out.print("Worker " + id.string() + " created.")


  fun ref assignNeighbors(neighbor: Worker) =>
    _neighbors.push(neighbor)

  fun ref countNeighbors() =>
    _neighbors.size()

  /* be gossip() =>
    if (_rumourCount < 10) and not _converged then
      _rumourCount = _rumourCount + 1
      if _rumourCount == 10 then
        _converged = true
        _supervisor.messageReceived()
      end
      let randomIndex = (I64(_neighbors.size()) * I64.rand()) % _neighbors.size()
      _neighbors(randomIndex)?.gossip()
    end */

  /* be pushSum(s: F64, w: F64) =>
    if not _converged then
      _sum = (_sum + s) / 2
      _weight = (_weight + w) / 2
      let estimate = _sum / _weight
      let randomIndex = (I64(_neighbors.size()) * I64.rand()) % _neighbors.size()
      _neighbors(randomIndex)?.pushSum(_sum, _weight)
    end */

actor Main
  var _supervisor: ConvergenceDetector
  var _workers: Array[Worker] = Array[Worker]
  var _totalNodes: I64 = 0
  let _env: Env

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
        initializeNeighbors() // Initialize neighbors in a line topology
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

  // Create Worker instances
  fun ref createWorkers() =>
    for i in Range[I64](0, _totalNodes) do
      _workers.push(Worker(_supervisor, i, _env))
    end


  fun ref initializeNeighbors() =>
    var neighbors = Array[Worker]

    for i in Range[I64](0, _totalNodes) do
      _env.out.print("Initializing neighbors for Worker " + i.string())

      var current: Worker

      try current = _workers(i.usize())? then _env.out.print('Error here') end

      _env.out.print(current.string())

      try
        if i > 0 then
          // neighbors.push(_workers(i.usize())?)
          current.assignNeighbors(_workers((i - 1).usize())?) // Left neighbor
        end

        if i < (_totalNodes - 1) then
          current.assignNeighbors(_workers((i + 1).usize())?) // Right neighbor
        end
      else
        _env.out.print("Couldn't assign neighbors")
      end

      _env.out.print("here>>>>" + current.countNeighbors.string())
    end
