package org.embulk.output.s3_parquet.aws


import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}


object Aws
{

    trait Task
        extends AwsCredentials.Task
            with AwsEndpointConfiguration.Task
            with AwsClientConfiguration.Task
            with AwsS3Configuration.Task

    def apply(task: Task): Aws =
    {
        new Aws(task)
    }

}

class Aws(task: Aws.Task)
{

    def withS3[A](f: AmazonS3 => A): A =
    {
        val builder: AmazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
        AwsS3Configuration(task).configureAmazonS3ClientBuilder(builder)
        val svc = createService(builder)
        try f(svc)
        finally svc.shutdown()
    }

    def withTransferManager[A](f: TransferManager => A): A =
    {
        withS3 { s3 =>
            val svc = TransferManagerBuilder.standard().withS3Client(s3).build()
            try f(svc)
            finally svc.shutdownNow(false)
        }
    }

    def withGlue[A](f: AWSGlue => A): A =
    {
        val builder: AWSGlueClientBuilder = AWSGlueClientBuilder.standard()
        val svc = createService(builder)
        try f(svc)
        finally svc.shutdown()
    }

    def createService[S <: AwsClientBuilder[S, T], T](builder: AwsClientBuilder[S, T]): T =
    {
        AwsEndpointConfiguration(task).configureAwsClientBuilder(builder)
        AwsClientConfiguration(task).configureAwsClientBuilder(builder)
        builder.setCredentials(AwsCredentials(task).createAwsCredentialsProvider)

        builder.build()
    }
}
