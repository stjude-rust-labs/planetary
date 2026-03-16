# planetary

`planetary` is a Kubernetes-based task executor for the Task Execution Service
(TES) specification.

## Configuration

### Orchestrator Storage

The `planetary` orchestrator service uses shared storage to communicate with
pods that execute TES tasks.

It is up to the cluster administrator to specify the manner of this shared
storage via Persistent Volume Claims (PVC).

Both the orchestrator and the task pods depend on the existence of an
`orchestrator-storage` PVC in their respective namespaces.

The volume referenced by the PVCs must use shared storage like a singular NFS
share or the same cloud storage bucket used as a file system.

After the `planetary` chart is installed, the orchestrator service will not be
scheduled until the `orchestrator-storage` PVC exists and is bound.

### Network Policies

Network policies are disabled for task pods by default.
In production deployments, it is recommended you enable network policies for
the task pods.

The default network policy allows communication to `kube-dns` on port 53 - both
UDP and TCP - and all other IPv4 traffic on port 443.

It is suggested that you review this network policy for your use case and
determine if there are requirements where this policy is not strict enough.

To fully lock down the task pods, we suggest a custom egress rule that allows
internet connectivity on port 443 for all IPv4 ranges except the private CIDR
ranges of your pods and services.

An example looks like:

```yaml
networkPolicies:
  enabled: true
  egressRules:
    - to:
      - ipBlock:
          cidr: 0.0.0.0/0
          except:
            - 10.244.0.0/16
            - 10.0.0.0/16
      ports:
        - port: 443
          protocol: TCP
```

This allows communication to cloud specific services such as IDMS on Azure and
any other private networks.

Restrict further as needed if other network calls should not be allowed.
