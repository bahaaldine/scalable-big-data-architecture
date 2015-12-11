docker-machine -D create \
  --driver amazonec2 \
  --amazonec2-access-key $AWS_ACCESS_KEY \
  --amazonec2-secret-key $AWS_SECRET_KEY \
  --amazonec2-vpc-id $AWS_VPC_ID \
  --amazonec2-zone b \
  baha-lambda-architecture