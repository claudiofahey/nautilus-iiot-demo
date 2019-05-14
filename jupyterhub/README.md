
See https://zero-to-jupyterhub.readthedocs.io.

Install Helm 2.13.1 (must be 2.11 or higher).

```
helm init --upgrade
export DOCKER_REPOSITORY=<hostname>:<port>/<namespace>
./build.sh
./deploy.sh
```

Browse to [JupyterHub](http://proxy-public.examples.svc.cluster.local).

# Reference

- https://zero-to-jupyterhub.readthedocs.io
- https://jupyter-docker-stacks.readthedocs.io
