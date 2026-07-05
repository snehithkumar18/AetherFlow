FROM gcr.io/oss-fuzz-base/base-builder-jvm

# Copy project source
COPY . $SRC/
WORKDIR $SRC/

# Copy build script
COPY .clusterfuzzlite/build.sh $SRC/
