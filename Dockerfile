FROM nvidia/cuda:10.2-devel-centos7

# Create EM user
ARG groupid
ARG userid
ARG groupname
RUN groupadd -r -g "${groupid}" "${groupname}" && useradd -r -M "${groupname}" -u "${userid}" -g "${groupname}"

RUN curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-$(uname)-$(uname -m).sh"
RUN bash Mambaforge-$(uname)-$(uname -m).sh -b -p "conda"

RUN source "/conda/etc/profile.d/conda.sh" && source "/conda/etc/profile.d/mamba.sh" && mamba create -c conda-forge -p /install/pythonenv python=3.9 pip libtiff=4.4.0 htcondor --override-channels -y

# Install Relion
RUN mkdir /install/relion
COPY python-relion /install/relion
RUN source "/conda/etc/profile.d/conda.sh" && conda activate /install/pythonenv && pip install zocalo
RUN source "/conda/etc/profile.d/conda.sh" && conda activate /install/pythonenv && pip install -e /install/relion

# Install pipeliner
RUN mkdir /install/ccpem-pipeliner
COPY ccpem-pipeliner /install/ccpem-pipeliner
RUN source "/conda/etc/profile.d/conda.sh" && conda activate /install/pythonenv && pip install -e /install/ccpem-pipeliner


RUN chown -R "${userid}":"${groupid}" install
