# Kubernetes fleet collector

The collector runs as one long-lived pod and gives every host in its YAML
`nodes` list a dedicated scrape thread and persistent libvirt connection.
Completed samples are handed to a shared Kafka publisher queue immediately.
The complete configuration is stored in a Secret because it includes the
Redpanda and libvirt credentials.

The Deployment uses the `Recreate` strategy and one replica. Do not change it
to `RollingUpdate` or run a second replica with the same node list: overlapping
collectors would publish duplicate metrics.

## Create the configuration Secret

From the repository root:

```bash
cp deploy/kubernetes/agent-network.example.yml deploy/agent.yml
chmod 600 deploy/agent.yml
# Replace credentials, endpoints, chassis names, and nodes.
${EDITOR:-vi} deploy/agent.yml

kubectl -n metrics create secret generic ovn-traffic-agent-config \
  --from-file=agent.yml=deploy/agent.yml \
  --dry-run=client -o yaml | kubectl apply -f -
```

`deploy/agent.yml` is gitignored. Do not commit a rendered Secret or a real
configuration file containing credentials.

## Deploy

```bash
kubectl -n metrics apply -f deploy/kubernetes/deployment.yml
kubectl -n metrics rollout status deployment/ovn-traffic-agent --timeout=2m
kubectl -n metrics logs -f deployment/ovn-traffic-agent
```

The manifest uses normal pod networking. The pod network must be able to reach
the compute management addresses, OVN SBDB, and Redpanda. Add `hostNetwork:
true` only if that routing is unavailable and using the Kubernetes node's
network namespace is the intended solution.

If `cr.ib.systems` requires explicit registry credentials, create an image-pull
Secret and add it to `spec.template.spec.imagePullSecrets`.

## Update configuration

The process reads YAML only at startup. Recreate the Secret and restart the
Deployment after changing `deploy/agent.yml`:

```bash
kubectl -n metrics create secret generic ovn-traffic-agent-config \
  --from-file=agent.yml=deploy/agent.yml \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n metrics rollout restart deployment/ovn-traffic-agent
kubectl -n metrics rollout status deployment/ovn-traffic-agent --timeout=2m
```

Because the strategy is `Recreate`, the old collector exits before its
replacement starts. The replacement establishes fresh counter baselines on
its first poll and begins publishing deltas on the following poll.

## Split a larger fleet

Create another Secret and Deployment name for each shard, with a disjoint
`nodes` list. Every agent scrapes all hosts assigned to its own file in
parallel. A compute node must belong to exactly one active collector.
