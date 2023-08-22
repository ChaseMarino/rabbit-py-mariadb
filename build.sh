# https://github.com/ucalgary/docker-python-librdkafka
GCREPO="registry.gitlab.com/{REPO}/gitlabregistries"
LOCALCONT="resultwriter:0.1.1"
docker build -t $LOCALCONT -f Dockerfile .
docker tag $LOCALCONT $GCREPO/$LOCALCONT
docker push $GCREPO/$LOCALCONT
