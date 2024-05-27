FROM mcr.microsoft.com/vscode/devcontainers/miniconda:0-3

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends gcc g++ \
    && apt-get clean

RUN conda install -n base -c conda-forge mamba

COPY .devcontainer/environment.yml* .devcontainer/noop.txt /tmp/conda-tmp/

RUN if [ -f "/tmp/conda-tmp/environment.yml" ]; then umask 0002 && conda env update -n base -f /tmp/conda-tmp/environment.yml; fi \
    && rm -rf /tmp/conda-tmp

RUN echo "source activate base" > ~/.bashrc  
# Copy environment.yml (if found) to a temp location so we update the environment. Also
# copy "noop.txt" so the COPY instruction does not fail if no environment.yml exists.

# [Optional] Uncomment to install a different version of Python than the default
# RUN conda install -y python=3.6 \
#     && pip install --no-cache-dir pipx \
#     && pipx reinstall-all
# [Optional] Uncomment this section to install additional OS packages.


