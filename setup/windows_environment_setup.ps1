$WORKSPACE_LOCATION="C:\Users\domin\projects\aws-glue-local-dev-and-test"
$SCRIPT_FILE_NAME="sample.py"
$UNIT_TEST_FILE_NAME="test_sample.py"
$AWS_FOLDER_LOCATION="C:\Users\domin\.aws"
$AWS_PROFILE="glue-dev"

docker run -it -v $AWS_FOLDER_LOCATION:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pyspark amazon/aws-glue-libs:glue_libs_3.0.0_image_01 pyspark
