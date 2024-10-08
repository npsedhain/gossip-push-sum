use "time"
use "collections"
use "random"

use @rand[I32]()
use @srand[None](seed: U32)

class Stopwatch
  var _start: U64 = 0
  var _running: Bool = false

  fun ref start() =>
    if not _running then
      _start = Time.nanos()
      _running = true
    end

  fun ref stop(): F64 =>
    if _running then
      let elapsed = (Time.nanos() - _start).f64() / 1_000_000
      _running = false
      elapsed
    else
      0
    end

primitive MathLib
  fun cube_root(value: F64): F64 =>
    if value == 0.0 then
      0.0
    else
      var result: F64 = value
      var x0: F64 = value / 3.0
      var x1: F64 = ((2.0 * x0) + (value / (x0 * x0))) / 3.0

      while (x0 - x1).abs() > 1e-7 do
        x0 = x1
        x1 = ((2.0 * x0) + (value / (x0 * x0))) / 3.0
      end
      x1
    end

actor Worker
  var _id: I64
  let _env: Env
  var _neighbors: Array[Worker tag]

  var _sum: F64 = 0
  var _weight: F64 = 1.0
  var _previous_ratio: F64 = 0.0
  var _stable_rounds: I64 = 0

  var _rumourCount: I64 = 0

  var _converged: Bool = false
  var _halted: Bool = false
  var _failed: Bool = false

  new create(env: Env, id: I64, failed: Bool) =>
    _id = id
    _neighbors = []
    _env = env
    _sum = (id+1).f64()
    _previous_ratio =_sum/_weight
    _failed = failed

  be startPushSum(delta: F64, main: Main) =>
    _sum = _sum / 2.0
    _weight = _weight / 2.0
    _previous_ratio = _sum / _weight

    let rand_value: I32 = @rand()
    let index: USize = (rand_value.abs().usize() % _neighbors.size().usize())
    try
      let randomNeighbor: Worker tag = _neighbors(index)?
      randomNeighbor.computePushSum(_sum, _weight, delta, main)
    else
      _env.out.print("Error accessing neighbor at index: " + index.string())
    end

  be halt() =>
    _halted = true

  be computePushSum(received_sum: F64, received_weight: F64, delta: F64, main: Main) =>
    if _halted then return end

    if _failed then
      _converged = true
      main.worker_converged(_id)
    end

    _sum = _sum + received_sum
    _weight = _weight + received_weight

    _sum = _sum / 2.0
    _weight = _weight / 2.0

    let current_ratio = _sum / _weight
    let ratio_diff = (current_ratio - _previous_ratio).abs()
    _previous_ratio = current_ratio

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

      sendToRandomNeighbor(_sum, _weight, delta, main)
    end

  fun ref sendToRandomNeighbor(sum: F64, weight: F64, delta: F64, main: Main) =>
    let index: USize = (@rand().abs().usize() % _neighbors.size().usize())
    try
      let neighbor: Worker tag = _neighbors(index)?
      neighbor.computePushSum(sum, weight, delta, main)
    else
      _env.out.print("Error accessing neighbors")
    end

  be startGossip(main: Main) =>
    if _halted then return end

    if _failed then
      _converged = true
      main.worker_converged(_id)
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
    let index: USize = (@rand().abs().usize() % _neighbors.size().usize())
    try
      let neighbor: Worker tag = _neighbors(index)?
      neighbor.startGossip(main)
    else
      _env.out.print("Error sending messages.")
    end

  be notify_ready(main: Main) =>
    main.worker_ready(this)

  be assignNeighborsLine(neighbor: Worker tag, main:Main, total_nodes: I64) =>
    neighborPusher(neighbor)
    if (_neighbors.size() == 2) or ((_id == 0) and (_neighbors.size() == 1)) or ((_id == (total_nodes - 1)) and (_neighbors.size() == 1)) then
      main.neighbor_assigned()
    end

  be assignNeighborsFull(neighbor: Worker tag, main: Main, total_nodes: I64) =>
    neighborPusher(neighbor)
    if _neighbors.size() == (total_nodes - 1).usize() then
      main.neighbor_assigned()
    end

  be assignNeighbors3D(neighbor_id: I64, neighbor: Worker tag, main: Main, total_nodes: I64, cube_root_no: I64, topology: String) =>
    neighborPusher(neighbor)

    // Calculate x, y, z positions for this worker
    let x = _id % cube_root_no
    let y = (_id / cube_root_no) % cube_root_no
    let z = _id / (cube_root_no * cube_root_no)

    // Determine the expected number of neighbors based on the position in the grid
    var expected_neighbors: USize = 6  // Default to interior node (6 neighbors)

    // Check for edges and corners, adjust the expected number of neighbors accordingly
    if (x == 0) or (x == (cube_root_no - 1)) then
      expected_neighbors = expected_neighbors - 1
    end
    if (y == 0) or (y == (cube_root_no - 1)) then
      expected_neighbors = expected_neighbors - 1
    end
    if (z == 0) or (z == (cube_root_no - 1)) then
      expected_neighbors = expected_neighbors - 1
    end

    // If all expected neighbors have been assigned, notify the main process
    if (_neighbors.size() == (expected_neighbors+1)) and (topology=="imp3D")  then
      main.neighbor_assigned()
    elseif (_neighbors.size() == expected_neighbors) and (topology=="3D") then
      main.neighbor_assigned()
    end

  fun ref neighborPusher(neighbor: Worker tag) =>
    _neighbors.push(neighbor)

actor Main
  var _workers: Array[Worker tag] = Array[Worker tag]
  var totalNodes: I64 = 0
  let _env: Env
  var _workers_ready: USize = 0
  var _neighbors_ready: USize = 0
  var _algorithm: String = ""
  var _topology: String = ""
  var _converged_workers: HashSet[I64, HashEq[I64]] = HashSet[I64, HashEq[I64]]
  var cube_root_no: I64 = 0
  var _stopwatch: Stopwatch
  var _failed_nodes: HashSet[I64, HashEq[I64]] iso = HashSet[I64, HashEq[I64]]

  fun ref getFailedNodes(total: I64) =>
    for i in Range[I64](0, total) do
      let index: I64 = (@rand().abs().usize() % totalNodes.usize()).i64()
      _failed_nodes.set(index)
    end

  new create(env: Env) =>
    _env = env
    _stopwatch = Stopwatch
    _converged_workers = HashSet[I64, HashEq[I64]].create()

    @srand(Time.nanos().u32())

    try
      totalNodes = _env.args(1)?.i64()?
      _topology = _env.args(2)?
      _algorithm = _env.args(3)?
      getFailedNodes(4000)
      createWorkers()
      _env.out.print("Total nodes: " + totalNodes.string() + "  Topology: " + _topology.string() + "  Algorithm: " + _algorithm.string())
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
      _env.out.print("All Neighbors Ready")
      startAlgorithm()
    end

  fun ref createWorkers() =>
   if (_topology == "3D") or (_topology=="imp3D") then
    let input_nodes = totalNodes
    let cube_root = MathLib.cube_root(totalNodes.f64()).ceil().i64()
    let total_3D_nodes = cube_root * cube_root * cube_root
    totalNodes = total_3D_nodes
    cube_root_no = cube_root
    _env.out.print("Creating workers...   Cube root: " + cube_root.string() + "   Input notes: " + input_nodes.string())
   end
   for i in Range[I64](0, totalNodes) do
      var failed = false
      if _failed_nodes.contains(i) then failed = true end
      let worker = Worker(_env, i, failed)
      _workers.push(worker)
      worker.notify_ready(this)
   end

  be worker_converged(worker_id: I64) =>
    try
      if not _converged_workers.contains(worker_id) then
        _converged_workers = _converged_workers.add(worker_id)
        if _converged_workers.size() == totalNodes.usize() then
          _env.out.print("Program converged at " + _stopwatch.stop().string() + "ms")
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
      _stopwatch.start()
      let index: USize = @rand().abs().usize() % _workers.size().usize()
      let starter_worker: Worker tag = _workers(index)?
      _env.out.print("------Starting Algorithm------")

      if _algorithm == "gossip" then
        starter_worker.startGossip(this)

      elseif _algorithm == "push-sum" then
        starter_worker.startPushSum(10e-10, this)

      else
        _env.out.print("Unknown algorithm: " + _algorithm)
      end
    else
      _env.out.print("Error: Could not start the algorithm, invalid worker access.")
    end

  fun ref initializeNeighbors() =>
    _env.out.print("-----Initializing Network-----")
    if _topology=="line" then
      initializeNeighborsLine()
    elseif _topology == "full" then
      initializeNeighborsFull()
    elseif (_topology == "3D") or (_topology == "imp3D") then
      initializeNeighbors3D()
    else
      _env.out.print("Unknown topology: " + _topology)
    end

  fun ref initializeNeighborsLine() =>
    try
      for i in Range[I64](0, totalNodes) do
        var current: Worker tag = _workers(i.usize())?
        if i > 0 then
          current.assignNeighborsLine(_workers((i - 1).usize())?, this, totalNodes) // Left neighbor
        end

        if i < (totalNodes - 1) then
          current.assignNeighborsLine(_workers((i + 1).usize())?, this, totalNodes) // Right neighbor
        end
      end
    else
      _env.out.print("Error assigning neighbors")
    end

  fun ref initializeNeighborsFull() =>
    try
      for i in Range[I64](0, totalNodes) do
        var current: Worker tag = _workers(i.usize())?
        for j in Range[I64](0, totalNodes) do
          if i != j then
            current.assignNeighborsFull(_workers(j.usize())?, this, totalNodes)
          end
        end
      end
    else
      _env.out.print("Error assigning full network neighbors.")
    end

  fun ref initializeNeighbors3D() =>
    try
      for i in Range[I64](0, totalNodes) do
        var current: Worker tag = _workers(i.usize())?
        //We made excluded ids so that we dont repeat the random neighbor when assigning for Imperfect 3D topology
        var excluded_ids= HashSet[I64, HashEq[I64]].create()
        excluded_ids=excluded_ids.add(i)

        // Compute x, y, z for the current worker
        let x = i % cube_root_no
        let y = (i / cube_root_no) % cube_root_no
        let z = i / (cube_root_no * cube_root_no)

        // Left neighbor (x-1, y, z) if x > 0
        if x > 0 then
          excluded_ids=excluded_ids.add(i-1)
          current.assignNeighbors3D(i - 1, _workers((i - 1).usize())?, this, totalNodes, cube_root_no, _topology)
        end

        // Right neighbor (x+1, y, z) if x < cube_root_no - 1
        if x < (cube_root_no - 1) then
          excluded_ids=excluded_ids.add(i+1)
          current.assignNeighbors3D(i + 1, _workers((i + 1).usize())?, this, totalNodes, cube_root_no,  _topology)
        end

        // Front neighbor (x, y-1, z) if y > 0
        if y > 0 then
          excluded_ids=excluded_ids.add(i - cube_root_no)
          current.assignNeighbors3D(i - cube_root_no, _workers((i - cube_root_no).usize())?, this, totalNodes, cube_root_no,  _topology)
        end

        // Back neighbor (x, y+1, z) if y < cube_root_no - 1
        if y < (cube_root_no - 1) then
          excluded_ids=excluded_ids.add(i + cube_root_no)
          current.assignNeighbors3D(i + cube_root_no, _workers((i + cube_root_no).usize())?, this, totalNodes, cube_root_no,  _topology)
        end

        // Top neighbor (x, y, z-1) if z > 0
        if z > 0 then
          excluded_ids=excluded_ids.add(i - (cube_root_no * cube_root_no))
          current.assignNeighbors3D(i - (cube_root_no * cube_root_no), _workers((i - (cube_root_no * cube_root_no)).usize())?, this, totalNodes, cube_root_no,  _topology)
        end

        // Bottom neighbor (x, y, z+1) if z < cube_root_no - 1
        if z < (cube_root_no - 1) then
          excluded_ids=excluded_ids.add(i + (cube_root_no * cube_root_no))
          current.assignNeighbors3D(i + (cube_root_no * cube_root_no), _workers((i + (cube_root_no * cube_root_no)).usize())?, this, totalNodes, cube_root_no,  _topology)
        end

        if _topology == "imp3D" then
          assignRandomNeighborImp3D(current, excluded_ids)
        end
      end
    else
      _env.out.print("Error assigning 3D neighbors.")
    end


  fun ref assignRandomNeighborImp3D(current: Worker tag, excluded_ids: HashSet[I64, HashEq[I64]]) =>
    var assigned: Bool = false
    while not assigned do
      let rand_index: I64 = (@rand() % totalNodes.i32()).i64()

      if not excluded_ids.contains(rand_index) then
        try
          current.assignNeighbors3D(rand_index, _workers(rand_index.usize())?, this, totalNodes, cube_root_no, _topology)
          assigned = true
        else
          _env.out.print("Error accessing random worker at index: " + rand_index.string())
        end
      end
    end
