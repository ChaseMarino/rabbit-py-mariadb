 docker run -it --net=host  -e DEBUG=1 -e ITEM_ID=abc -v `pwd`:/mnt --entrypoint /bin/bash resultwriter:0.1.1
