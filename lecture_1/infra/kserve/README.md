# KServe
## knative v1.9.3 [참고](https://github.com/knative/serving/releases/tag/knative-v1.9.3)
- https://github.com/knative/serving/releases/download/knative-v1.9.3/serving-crds.yaml
- https://github.com/knative/serving/releases/download/knative-v1.9.3/serving-core.yaml

```
kubectl apply -f serving-crds.yaml
kubectl apply -f serving-core.yaml
```
## istio v1.15 [참고](https://istio.io/latest/docs/setup/getting-started/)
```
curl -L https://istio.io/downloadIstio | ISTIO_VERSION=1.15.0 TARGET_ARCH=x86_64 sh -
cd ./istio-1.15.0/bin
./istioctl install -y

```
### istio for knative [참고](https://github.com/knative-extensions/net-istio/releases/tag/knative-v1.9.3)
- https://github.com/knative-extensions/net-istio/releases/download/knative-v1.9.3/net-istio.yaml

```
kubectl apply -f net-istio.yaml
```

## cert-manager [참고](https://cert-manager.io/docs/installation/)
- https://github.com/cert-manager/cert-manager/releases/download/v1.10.0/cert-manager.yaml

```
kubectl apply -f cert-manager.yaml
```

## kserve v1.10.0 [참고](https://kserve.github.io/website/0.10/admin/serverless/serverless/#4-install-kserve)
- https://github.com/kserve/kserve/releases/download/v0.10.0/kserve.yaml

```
kubectl apply -f kserve.yaml
```