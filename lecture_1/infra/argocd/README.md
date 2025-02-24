# ArgoCD
## ArgoCD [참고](https://argo-cd.readthedocs.io/en/stable/getting_started/)
- https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

```
kubectl create namespace argocd
kubectl apply -f argocd.yaml -n argocd
kubectl patch svc argocd-server -n argocd -p '{"spec": {"type": "LoadBalancer"}}'
# patch가 안되면 edit으로 변경 후 저장

kubectl get secrets argocd-initial-admin-secret -n argocd -o yaml
```