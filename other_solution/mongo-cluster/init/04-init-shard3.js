rs.initiate({
  _id: "shard3RS",
  members: [
    { _id: 0, host: "shard3a:27017" },
    { _id: 1, host: "shard3b:27017" },
    { _id: 2, host: "shard3c:27017" }
  ]
})