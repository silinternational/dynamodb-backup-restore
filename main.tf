locals {
  # B2 environment variables (only set if B2 backup is enabled)
  b2_env_vars = var.b2_backup_enabled ? {
    B2_APPLICATION_KEY_ID = var.b2_application_key_id
    B2_APPLICATION_KEY    = var.b2_application_key
    B2_BUCKET_NAME        = var.b2_bucket
    B2_ENDPOINT           = var.b2_endpoint
  } : {}

  short_env_names = {
    "staging"    = "stg"
    "production" = "prod"
  }
  short_env_name = local.short_env_names[var.environment]
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Reference existing S3 bucket (created manually)
data "aws_s3_bucket" "mfa_backups" {
  bucket = var.backup_bucket_name
}

# Archive Lambda functions
data "archive_file" "daily_backup" {
  type             = "zip"
  source_dir       = "${path.module}/lambda/daily_backup"
  output_file_mode = "0666"
  output_path      = "daily_backup_${local.short_env_name}.zip"
  excludes         = ["*.pyc", "__pycache__"]
}

data "archive_file" "disaster_recovery" {
  type             = "zip"
  source_dir       = "${path.module}/lambda/disaster_recovery"
  output_file_mode = "0666"
  output_path      = "disaster_recovery_${local.short_env_name}.zip"
  excludes         = ["*.pyc", "__pycache__"]
}

resource "aws_s3_bucket_versioning" "mfa_backups" {
  bucket = data.aws_s3_bucket.mfa_backups.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mfa_backups" {
  bucket = data.aws_s3_bucket.mfa_backups.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "mfa_backups" {
  bucket                  = data.aws_s3_bucket.mfa_backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "mfa_backups" {
  bucket = data.aws_s3_bucket.mfa_backups.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      {
        Sid    = "DynamoDBImportAccess"
        Effect = "Allow"
        Principal = {
          Service = "dynamodb.amazonaws.com"
        }
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          data.aws_s3_bucket.mfa_backups.arn,
          "${data.aws_s3_bucket.mfa_backups.arn}/*"
        ]
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      },

      {
        Sid    = "LambdaAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
            aws_iam_role.daily_backup_lambda_role.arn,
            aws_iam_role.disaster_recovery_lambda_role.arn
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
          "s3:ListBucketVersions"
        ]
        Resource = [
          data.aws_s3_bucket.mfa_backups.arn,
          "${data.aws_s3_bucket.mfa_backups.arn}/*"
        ]
      }
    ]
  })
}

# S3 Lifecycle policy
resource "aws_s3_bucket_lifecycle_configuration" "mfa_backups" {
  bucket = data.aws_s3_bucket.mfa_backups.id

  rule {
    id     = "${var.app_name}_backup_lifecycle_${local.short_env_name}"
    status = "Enabled"

    filter {
      prefix = "native-exports/"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.backup_retention_days
    }
  }
}

# IAM Role for Backup Lambda
resource "aws_iam_role" "daily_backup_lambda_role" {
  name = "${var.app_name}-daily-backup-lambda-role-${local.short_env_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "daily_backup_lambda_policy" {
  name = "${var.app_name}-daily-backup-lambda-policy-${local.short_env_name}"
  role = aws_iam_role.daily_backup_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:ExportTableToPointInTime",
          "dynamodb:DescribeExport",
          "dynamodb:DescribeTable",
          "dynamodb:ListExports",
          "dynamodb:DescribeContinuousBackups"
        ]
        Resource = concat(

          [for table_name in var.dynamodb_tables :
            "arn:aws:dynamodb:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${table_name}"
          ],

          [for table_name in var.dynamodb_tables :
            "arn:aws:dynamodb:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${table_name}/export/*"
          ]
        )
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          data.aws_s3_bucket.mfa_backups.arn,
          "${data.aws_s3_bucket.mfa_backups.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sts:GetCallerIdentity"
        ]
        Resource = "*"
      },
      {
        # Lets the function re-invoke itself asynchronously to reconcile the
        # manifest once an export that was still running when we stopped
        # watching it reaches a terminal state. Built from the deterministic
        # function name (rather than the resource's .arn) to avoid a
        # dependency cycle with aws_lambda_function.daily_backup, which
        # depends_on this policy.
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "arn:aws:lambda:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:function:${var.app_name}-daily-backup-${local.short_env_name}"
      }
    ]
  })
}

# Backup Lambda Function
resource "aws_lambda_function" "daily_backup" {
  filename         = data.archive_file.daily_backup.output_path
  function_name    = "${var.app_name}-daily-backup-${local.short_env_name}"
  description      = "${var.app_name} Backup Lambda for ${var.environment}"
  role             = aws_iam_role.daily_backup_lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = var.lambda_runtime
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_size
  source_code_hash = data.archive_file.daily_backup.output_base64sha256

  environment {
    variables = merge({
      BACKUP_BUCKET   = data.aws_s3_bucket.mfa_backups.bucket
      ENVIRONMENT     = local.short_env_name
      DYNAMODB_TABLES = jsonencode(var.dynamodb_tables)
    }, local.b2_env_vars)
  }

  depends_on = [
    aws_iam_role_policy.daily_backup_lambda_policy,
    aws_cloudwatch_log_group.daily_backup_logs,
  ]
}

resource "aws_cloudwatch_log_group" "daily_backup_logs" {
  name              = "/aws/lambda/${var.app_name}-daily-backup-${local.short_env_name}"
  retention_in_days = 30
}

resource "aws_cloudwatch_event_rule" "daily_backup_schedule" {
  count               = var.backup_schedule_enabled ? 1 : 0
  name                = "${var.app_name}-daily-backup-schedule-${local.short_env_name}"
  description         = "Trigger ${var.app_name} backup for ${var.environment}"
  schedule_expression = var.backup_schedule
}

resource "aws_cloudwatch_event_target" "daily_backup_target" {
  count     = var.backup_schedule_enabled ? 1 : 0
  rule      = aws_cloudwatch_event_rule.daily_backup_schedule[0].name
  target_id = "${title(var.app_name)}DailyBackupTarget"
  arn       = aws_lambda_function.daily_backup.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.backup_schedule_enabled ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daily_backup.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_backup_schedule[0].arn
}

# IAM Role for Disaster Recovery Lambda
resource "aws_iam_role" "disaster_recovery_lambda_role" {
  name = "${var.app_name}-disaster-recovery-lambda-role-${local.short_env_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Disaster recovery Lambda policy
resource "aws_iam_role_policy" "disaster_recovery_lambda_policy" {
  name = "${var.app_name}-disaster-recovery-lambda-policy-${local.short_env_name}"
  role = aws_iam_role.disaster_recovery_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:*"
      },

      {
        Effect = "Allow"
        Action = [
          "dynamodb:CreateTable",
          "dynamodb:DescribeTable",
          "dynamodb:PutItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:UpdateTable",
          "dynamodb:DeleteTable",
          "dynamodb:ListTables",
          "dynamodb:DescribeContinuousBackups",
          "dynamodb:UpdateContinuousBackups",
          "dynamodb:ListTagsOfResource",
          "dynamodb:TagResource",
          "dynamodb:UntagResource",
          "dynamodb:Scan",
          "dynamodb:DeleteItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:UpdateItem"
        ]
        Resource = concat(
          [for table_name in var.dynamodb_tables :
            "arn:aws:dynamodb:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${table_name}"
          ],
          [for table_name in var.dynamodb_tables :
            "arn:aws:dynamodb:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${table_name}/index/*"
          ]
        )
      },

      {
        Effect = "Allow"
        Action = [
          "dynamodb:ImportTable",
          "dynamodb:DescribeImport",
          "dynamodb:ListImports"
        ]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeGlobalTable",
          "dynamodb:CreateGlobalTable",
          "dynamodb:UpdateGlobalTable"
        ]
        Resource = "*"
      },


      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetObjectVersion",
          "s3:ListBucketVersions"
        ]
        Resource = [
          data.aws_s3_bucket.mfa_backups.arn,
          "${data.aws_s3_bucket.mfa_backups.arn}/*"
        ]
      },

      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "${var.app_name}/DisasterRecovery"
          }
        }
      },

      {
        # Lets the function re-invoke itself asynchronously to continue a restore
        # that's still running when the Lambda is about to hit its timeout, rather
        # than being killed mid-write. Built from the deterministic function name
        # (rather than the resource's .arn) to avoid a dependency cycle with
        # aws_lambda_function.disaster_recovery, which depends_on this policy.
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "arn:aws:lambda:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:function:${var.app_name}-disaster-recovery-${local.short_env_name}"
      },
    ]
  })
}

# Disaster Recovery Lambda Function
resource "aws_lambda_function" "disaster_recovery" {
  filename         = data.archive_file.disaster_recovery.output_path
  function_name    = "${var.app_name}-disaster-recovery-${local.short_env_name}"
  description      = "${var.app_name} Disaster Recovery Lambda for ${var.environment}"
  role             = aws_iam_role.disaster_recovery_lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = var.lambda_runtime
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_size
  source_code_hash = data.archive_file.disaster_recovery.output_base64sha256

  environment {
    variables = merge({
      BACKUP_BUCKET   = data.aws_s3_bucket.mfa_backups.bucket
      ENVIRONMENT     = local.short_env_name
      DYNAMODB_TABLES = jsonencode(var.dynamodb_tables)
      }, var.restore_storage_mode == "b2" ? {
      B2_APPLICATION_KEY_ID = var.b2_application_key_id
      B2_APPLICATION_KEY    = var.b2_application_key
      B2_BUCKET_NAME        = var.b2_bucket
      B2_ENDPOINT           = var.b2_endpoint
    } : {})
  }

  depends_on = [
    aws_iam_role_policy.disaster_recovery_lambda_policy,
    aws_cloudwatch_log_group.disaster_recovery_logs,
  ]
}

# CloudWatch Log Group for Disaster Recovery
resource "aws_cloudwatch_log_group" "disaster_recovery_logs" {
  name              = "/aws/lambda/${var.app_name}-disaster-recovery-${local.short_env_name}"
  retention_in_days = 30
}
