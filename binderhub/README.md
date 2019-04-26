
See https://binderhub.readthedocs.io.

Install Helm 2.13.1 (must be 2.11 or higher).

```
helm init --upgrade
binder/build.sh
./deploy.sh
```

Browse to [Binder](http://binder.examples.svc.cluster.local).

Github repository name or URL: https://github.com/binder-examples/conda

Git branch, tag or commit: 43730859d5bc7441de486e8f66219a001e2e13a3

Once environment has been built, you will be redirected to 
[JupyterHub](http://proxy-public.examples.svc.cluster.local).
