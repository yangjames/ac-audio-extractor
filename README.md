# Audio Commons Audio Extractor

Fork of the Audio Commons Audio Extractor.

To build docker image:
```
docker build -t <image name> .
```

To run:
```
docker run -it --rm -v <local path to data>:/audio -v <desired path to output JSON files>:/outdir soundry/ac-audio-extractor -i /audio/ -o /outdir/
```

If the desired output path doesn't already exist, the path will be created.