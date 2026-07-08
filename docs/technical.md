# **Groupnet: Technical Overview**

Groupnet is a **deterministic, leaderless coordination fabric** designed for distributed systems that partition state into shard groups. It provides **group‑local membership**, **implicit coordinator selection**, **inter‑group awareness**, and **synchronized shard‑scoped operations** without relying on consensus protocols such as Raft or Paxos.

Groupnet enables distributed databases, search engines, vector stores, and time‑series systems to implement shard‑level orchestration without embedding bespoke coordination logic or maintaining global consensus.

---

## **1. System Model**

### **1.1 Groups**
A *group* is a logical set of nodes responsible for a shard or partition.  
Each group maintains:

- a convergent membership view  
- a deterministic coordinator  
- shard‑local metadata  
- versioning state  
- routing information  
- **knowledge of other groups for cross‑shard routing**  

Groups operate independently for coordination but maintain **global awareness** for routing.

### **1.2 Inter‑Group Awareness**
Groups maintain a lightweight, cluster‑wide map describing:

- which group owns which resource or key‑range  
- coordinator identities for each group  
- routing metadata required to forward requests to the correct owner  

This enables any node to route a request to the correct group without global consensus.

### **1.3 Nodes**
Nodes participate in one or more groups.  
Each node maintains:

- local membership state  
- local metadata cache  
- coordinator selection logic  
- inter‑group routing tables  
- transport bindings (RPC)  

Nodes do not maintain logs or term histories.

---

## **2. Coordinator Model**

Groupnet uses **implicit coordinator selection**, not leader election.

### **2.1 Deterministic Selection**
The coordinator is chosen using a deterministic rule, such as:

- lowest node ID  
- highest priority  
- stable hash ordering  

All nodes converge on the same coordinator without voting.

### **2.2 Non‑authoritative Coordinator**
The coordinator:

- does **not** own a write‑ahead log  
- does **not** enforce global ordering  
- does **not** commit entries  
- does **not** require quorum agreement  

It acts as a **lightweight orchestrator** for shard‑local operations.

---

## **3. Membership Management**

Groupnet maintains group membership using **gossip‑based dissemination**.

### **3.1 Convergence**
Nodes exchange membership deltas until all replicas converge on:

- the same membership set  
- the same coordinator  
- the same shard metadata  
- the same inter‑group routing map  

### **3.2 Failure Handling**
Failures are detected via:

- heartbeat timeouts  
- gossip suspicion  
- transport‑level disconnects  

Membership changes propagate deterministically.

---

## **4. Synchronization Model**

Groupnet provides **synchronized shard‑local operations** without consensus.

### **4.1 Operation Context**
Operations run inside a *synchronization context*:

```rust
group.sync(|ctx| {
    ctx.update_metadata("routing", "v3");
});
