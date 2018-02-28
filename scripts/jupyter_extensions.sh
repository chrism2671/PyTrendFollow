conda install -c conda-forge jupyter_contrib_nbextensions
jupyter contrib nbextension install --user
jupyter serverextension enable jupyter_nbextensions_configurator
jupyter nbextension enable ExecuteTime

conda install ipyparallel
jupyter serverextension enable --py ipyparallel
jupyter nbextension install ipyparallel --user --py
jupyter nbextension enable ipyparallel --user --py