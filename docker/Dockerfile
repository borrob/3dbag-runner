FROM 3dgi/geoflow-bundle-builder:2025.02.12 AS builder

# Download Dutch transformation grids
RUN wget https://cdn.proj.org/nl_nsgi_nlgeo2018.tif -O /usr/local/share/proj/nl_nsgi_nlgeo2018.tif && \
    wget https://cdn.proj.org/nl_nsgi_rdtrans2018.tif -O /usr/local/share/proj/nl_nsgi_rdtrans2018.tif


ENV PATH="/app/.venv/bin:$PATH"
ENV GF_PLUGIN_FOLDER="/usr/local/lib/geoflow-plugins"
ENV TYLER_RESOURCES_DIR="/app/assets"

RUN apt-get update && apt-get install -y curl
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*


# Install Binaries
COPY docker/binaries/roofer /usr/bin/roofer
COPY docker/binaries/cjseq /usr/bin/cjseq
COPY docker/binaries/reconstruct /usr/bin/reconstruct
COPY docker/binaries/tyler /usr/bin/tyler

# Install python environment
RUN curl -LsSf https://astral.sh/uv/install.sh | /bin/sh
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app
COPY src /app/.
COPY docker/tyler /app/assets

COPY pyproject.toml /app
COPY mypy.ini /app
COPY uv.lock /app
COPY docker/metadata.city.json /metadata.city.json
COPY docker/binaries/proj.db /usr/share/proj/proj.db
COPY docker/ahn.json /ahn.json

RUN uv sync

RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs

RUN npm install -g 3d-tiles-tools
