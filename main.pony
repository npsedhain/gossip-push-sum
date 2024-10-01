use "collections"
 

use @rand[I32]()
use @srand[None](seed: U32)
use @time[U64]()

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
  var previous_ratio: F64 = 0.0
  var _stable_rounds: I64 = 0
  var _halted: Bool = false

  new create(env: Env, supervisor: ConvergenceDetector, id: I64) =>
    _supervisor = supervisor
    _id = id
    _neighbors = []
    _env = env
    _sum = (id+1).f64()
    previous_ratio =_sum/_weight


  be countNeighbors(main: Main) =>
    main.receive_neighbor_count(_id, _neighbors.size())
  
  be startPushSum(delta: F64, main: Main) =>
    let rand_value: I32 = @rand()
    let index: USize = (rand_value.abs().usize() % _neighbors.size().usize())
    let half_sum = _sum / 2.0
    let half_weight = _weight / 2.0
    _sum = half_sum
    _weight = half_weight
    previous_ratio = _sum / _weight

    try
      let randomNeighbor: Worker tag = _neighbors(index)?
      randomNeighbor.computePushSum(half_sum, half_weight, delta, main)
    else
      _env.out.print("Error accessing neighbor at index: " + index.string())
    end
  
  be halt() =>
    _halted = true
  
  be computePushSum(received_sum: F64, received_weight: F64, delta: F64, main: Main) =>
    if _halted then
      return
    end
    let newsum = _sum + received_sum
    let newweight = _weight + received_weight
    let current_ratio = _sum / _weight
    let ratio_diff = (previous_ratio - current_ratio).abs()

    if _converged then
      sendToRandomNeighbor(received_sum, received_weight, delta, main)
    else
      if ratio_diff > delta then
        _stable_rounds = 0
      else
        _stable_rounds = _stable_rounds + 1
      end

      if _stable_rounds >= 3 then
        _converged = true
        main.worker_converged(_id)
      end

      
      _sum = newsum / 2.0
      _weight = newweight / 2.0
      previous_ratio = _sum/_weight

      sendToRandomNeighbor(_sum, _weight, delta, main)
      
    end
  
  fun ref sendToRandomNeighbor(sum: F64, weight: F64, delta: F64, main: Main) =>
    let rand_value: I32 = @rand()
    let index: USize = (rand_value.abs().usize() % _neighbors.size().usize())
    try
      let randomNeighbor: Worker tag = _neighbors(index)?
      randomNeighbor.computePushSum(sum, weight, delta, main)
    else
      _env.out.print("Error accessing neighbor at index: " + index.string())
    end

  be startGossip(main: Main) =>
    if _halted then
      return
    end
  
    if _converged then 
      gossipToRandomNeighbor(main)
    else
      _rumourCount = _rumourCount + 1

      if _rumourCount >= 10 then
        _converged = true
        main.worker_converged(_id)
      end
      gossipToRandomNeighbor(main)
    end

  
  fun ref gossipToRandomNeighbor(main: Main) =>
    let rand_value: I32 = @rand()
    let index: USize = (rand_value.abs().usize() % _neighbors.size().usize())
    try
      let randomNeighbor: Worker tag = _neighbors(index)?
      randomNeighbor.startGossip(main)
    else
      _env.out.print("Error accessing neighbor at index: " + index.string())
    end

  be notify_ready(main: Main) =>
    main.worker_ready(this)

  be assignNeighbors(neighbor: Worker tag, main:Main, total_nodes: I64) =>
    _neighbors.push(neighbor)
    if (_neighbors.size() == 2) or ((_id == 0) and (_neighbors.size() == 1)) or ((_id == (total_nodes - 1)) and (_neighbors.size() == 1)) then
      main.neighbor_assigned()  
    end

actor Main
  var _supervisor: ConvergenceDetector
  var _workers: Array[Worker tag] = Array[Worker tag]
  var totalNodes: I64 = 0
  let _env: Env
  var _workers_ready: USize = 0
  var _neighbors_ready: USize = 0
  var _algorithm: String = ""
  var converged_workers: HashSet[I64, HashEq[I64]] = HashSet[I64, HashEq[I64]]

  new create(env: Env) =>
    converged_workers = HashSet[I64, HashEq[I64]].create()
    env.out.print("Converged Worker Count: " + converged_workers.size().string()) 
    _env = env
    _supervisor = ConvergenceDetector(env)
    @srand(@time().u32())
    try
      totalNodes = _env.args(1)?.i64()?
      let topology = _env.args(2)?
      let algorithm = _env.args(3)?
      _algorithm=algorithm

      if topology == "line" then
        createWorkers()
        _env.out.print("Number of workers created: " + _workers.size().string()) // Check array size
        _env.out.print("Total nodes: " + totalNodes.string())
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
    if _workers_ready == totalNodes.usize() then
      initializeNeighbors()
    end
  
  be neighbor_assigned() =>
    _neighbors_ready = _neighbors_ready + 1
    if _neighbors_ready == totalNodes.usize() then
       
      _env.out.print("Converged Worker")

      startAlgorithm() 
    end

  fun ref createWorkers() =>
    for i in Range[I64](0, totalNodes) do
      let worker = Worker(_env, _supervisor, i)
      _workers.push(worker)
      worker.notify_ready(this)
    end
  

  be worker_converged(worker_id: I64) =>
    _env.out.print("Worker " + worker_id.string() + " has converged.")
    try
      if not converged_workers.contains(worker_id) then
      
        converged_workers = converged_workers.add(worker_id)
        if converged_workers.size() == totalNodes.usize() then
          for i in Range[I64](0, totalNodes) do
            let current: Worker tag = _workers(i.usize())?
            current.halt() // Send halt message to each worker
          end  
        end
      end
    else
       _env.out.print("Error checking or adding worker convergence.")
    end

  fun ref startAlgorithm() =>
    try
      if _algorithm == "gossip" then
        _env.out.print("Starting Gossip Algorithm.")
     
        let starter_worker: Worker tag = _workers(0)?
        starter_worker.startGossip(this)

      elseif _algorithm == "push-sum" then
        _env.out.print("Starting Push-Sum Algorithm.")
      
        let starter_worker: Worker tag = _workers(0)?
        starter_worker.startPushSum(10e-10,this)

      else
      _env.out.print("Unknown algorithm: " + _algorithm)
      end
    else
   
      _env.out.print("Error: Could not start the algorithm, invalid worker access.")
      end

  fun ref initializeNeighbors() =>
    var neighbors = Array[Worker tag]

    try
      for i in Range[I64](0, totalNodes) do
        _env.out.print("Initializing neighbors for Worker " + i.string())

        var current: Worker tag = _workers(i.usize())?

        if i > 0 then
          current.assignNeighbors(_workers((i - 1).usize())?, this, totalNodes) // Left neighbor
        end

        if i < (totalNodes - 1) then
          current.assignNeighbors(_workers((i + 1).usize())?, this, totalNodes) // Right neighbor
        end

        current.countNeighbors(this)

      end
    else
      _env.out.print("Error assigning neighbors")
    end

  be receive_neighbor_count(worker_id: I64, count: USize) =>
    _env.out.print("Worker " + worker_id.string() + " has " + count.string() + " neighbors")
    