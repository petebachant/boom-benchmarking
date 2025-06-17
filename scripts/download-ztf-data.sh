mkdir -p data

FNAME=ztf_public_20250614.tar.gz

# Only download the file if it does not already exist
if [ -f data/${FNAME} ]; then
    echo "File data/${FNAME} already exists; Skipping download"
else
    echo "Downloading ZTF data to data/${FNAME}"
    curl -o data/${FNAME} https://ztf.uw.edu/alerts/public/${FNAME}
fi

# Data directory is the file name without extension
FNAME_NO_EXT=${FNAME%.tar.gz}

mkdir -p data/${FNAME_NO_EXT}

tar -xzf data/${FNAME} -C data/${FNAME_NO_EXT}
