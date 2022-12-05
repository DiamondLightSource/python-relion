FROM python:3.9

# Install Relion
ENV VIRTUAL_ENV=/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY . .
RUN python -m pip install zocalo procrunner
# Will eventually just include 'relion' in the above step
RUN python -m pip install -e .
