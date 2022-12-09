FROM nvidia/cuda:10.2-devel-centos7

# Create EM user
RUN groupadd -r -g 37829 k8s-em && useradd -r -M k8s-em -u 37679 -g k8s-em

RUN curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-$(uname)-$(uname -m).sh"
RUN bash Mambaforge-$(uname)-$(uname -m).sh -b -p "conda"

RUN source "/conda/etc/profile.d/conda.sh" && source "/conda/etc/profile.d/mamba.sh" && mamba create -c conda-forge -p /install/pythonenv python=3.9 pip libtiff --override-channels -y

# Install Relion
RUN mkdir /install/relion
COPY . /install/relion
RUN source "/conda/etc/profile.d/conda.sh" && conda activate /install/pythonenv && pip install zocalo procrunner
RUN source "/conda/etc/profile.d/conda.sh" && conda activate /install/pythonenv && pip install -e /install/relion

RUN chown -R 37679:37829 install
