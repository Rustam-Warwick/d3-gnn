kubectl delete -f flink-configuration-configmap.yaml
kubectl delete -f jobmanager-service.yaml
kubectl delete -f jobmanager-session-deployment-non-ha.yaml
kubectl delete -f taskmanager-session-deployment.yaml
kubectl create -f flink-configuration-configmap.yaml
kubectl create -f jobmanager-service.yaml
kubectl create -f jobmanager-session-deployment-non-ha.yaml
kubectl create -f taskmanager-session-deployment.yaml
