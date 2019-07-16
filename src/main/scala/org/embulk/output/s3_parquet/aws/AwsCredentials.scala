package org.embulk.output.s3_parquet.aws


import java.util.Optional

import com.amazonaws.auth.{AnonymousAWSCredentials, AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials, BasicSessionCredentials, DefaultAWSCredentialsProviderChain, EC2ContainerCredentialsProviderWrapper, EnvironmentVariableCredentialsProvider, STSAssumeRoleSessionCredentialsProvider, SystemPropertiesCredentialsProvider}
import com.amazonaws.auth.profile.{ProfileCredentialsProvider, ProfilesConfigFile}
import org.embulk.config.{Config, ConfigDefault, ConfigException}
import org.embulk.output.s3_parquet.aws.AwsCredentials.Task
import org.embulk.spi.unit.LocalFile


object AwsCredentials
{

    trait Task
    {

        @Config("auth_method")
        @ConfigDefault("\"default\"")
        def getAuthMethod: String

        @Config("access_key_id")
        @ConfigDefault("null")
        def getAccessKeyId: Optional[String]

        @Config("secret_access_key")
        @ConfigDefault("null")
        def getSecretAccessKey: Optional[String]

        @Config("session_token")
        @ConfigDefault("null")
        def getSessionToken: Optional[String]

        @Config("profile_file")
        @ConfigDefault("null")
        def getProfileFile: Optional[LocalFile]

        @Config("profile_name")
        @ConfigDefault("\"default\"")
        def getProfileName: String

        @Config("role_arn")
        @ConfigDefault("null")
        def getRoleArn: Optional[String]

        @Config("role_session_name")
        @ConfigDefault("null")
        def getRoleSessionName: Optional[String]

        @Config("role_external_id")
        @ConfigDefault("null")
        def getRoleExternalId: Optional[String]

        @Config("role_session_duration_seconds")
        @ConfigDefault("null")
        def getRoleSessionDurationSeconds: Optional[Int]

        @Config("scope_down_policy")
        @ConfigDefault("null")
        def getScopeDownPolicy: Optional[String]

    }

    def apply(task: Task): AwsCredentials =
    {
        new AwsCredentials(task)
    }
}

class AwsCredentials(task: Task)
{

    def createAwsCredentialsProvider: AWSCredentialsProvider =
    {
        task.getAuthMethod match {
            case "basic" =>
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                    getRequiredOption(task.getAccessKeyId, "access_key_id"),
                    getRequiredOption(task.getAccessKeyId, "secret_access_key")
                    ))

            case "env" =>
                new EnvironmentVariableCredentialsProvider

            case "instance" =>
                // NOTE: combination of InstanceProfileCredentialsProvider and ContainerCredentialsProvider
                new EC2ContainerCredentialsProviderWrapper

            case "profile" =>
                if (task.getProfileFile.isPresent) {
                    val pf: ProfilesConfigFile = new ProfilesConfigFile(task.getProfileFile.get().getFile)
                    new ProfileCredentialsProvider(pf, task.getProfileName)
                }
                else new ProfileCredentialsProvider(task.getProfileName)

            case "properties" =>
                new SystemPropertiesCredentialsProvider

            case "anonymous" =>
                new AWSStaticCredentialsProvider(new AnonymousAWSCredentials)

            case "session" =>
                new AWSStaticCredentialsProvider(new BasicSessionCredentials(
                    getRequiredOption(task.getAccessKeyId, "access_key_id"),
                    getRequiredOption(task.getSecretAccessKey, "secret_access_key"),
                    getRequiredOption(task.getSessionToken, "session_token")
                    ))

            case "assume_role" =>
                // NOTE: Are http_proxy, endpoint, region required when assuming role?
                val builder = new STSAssumeRoleSessionCredentialsProvider.Builder(
                    getRequiredOption(task.getRoleArn, "role_arn"),
                    getRequiredOption(task.getRoleSessionName, "role_session_name")
                    )
                task.getRoleExternalId.ifPresent(v => builder.withExternalId(v))
                task.getRoleSessionDurationSeconds.ifPresent(v => builder.withRoleSessionDurationSeconds(v))
                task.getScopeDownPolicy.ifPresent(v => builder.withScopeDownPolicy(v))

                builder.build()

            case "default" =>
                new DefaultAWSCredentialsProviderChain

            case am =>
                throw new ConfigException(s"'$am' is unsupported: `auth_method` must be one of ['basic', 'env', 'instance', 'profile', 'properties', 'anonymous', 'session', 'assume_role', 'default'].")
        }
    }

    private def getRequiredOption[A](o: Optional[A],
                                     name: String): A =
    {
        o.orElseThrow(() => new ConfigException(s"`$name` must be set when `auth_method` is ${task.getAuthMethod}."))
    }


}
