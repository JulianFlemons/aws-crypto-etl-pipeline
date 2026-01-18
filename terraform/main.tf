# Bucket creation
resource "aws_s3_bucket" "my_s3_bucket"{
    bucket = "julian-crypto-s3-bucket"

    tags = {
    Name = "My bucket"
    Enviroment ="Dev"
}
}