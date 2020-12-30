using System;
using System.IO;
using System.Threading.Tasks;
using System.Web;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using NetVips;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace aws_lambda_s3_img_resize
{
    public class Function
    {
        private string Key { get; set; }

        IAmazonS3 S3Client { get; } 

        public Function()
        {
            S3Client = new AmazonS3Client();
        }
         
        public Function(IAmazonS3 s3Client)
        {
            S3Client = s3Client;
        }
         
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return null;
            }

            try
            {
                Key = HttpUtility.UrlDecode(s3Event.Object.Key);
                var response = await S3Client.GetObjectAsync(s3Event.Bucket.Name, Key);

                if (!int.TryParse(Environment.GetEnvironmentVariable("Quality"), out var quality))
                {
                    quality = 80;
                }

                if (!bool.TryParse(Environment.GetEnvironmentVariable("IsWebP"), out var isOutWebP))
                {
                    isOutWebP = false;
                }

                await using var stream = response.ResponseStream;

                var img = Image.NewFromStream(stream);

                var width1 = response.Metadata["width1"];
                if (width1 == null)
                {
                    if (!int.TryParse(Environment.GetEnvironmentVariable("Width"), out var width))
                    {
                        width = 300;
                    }
                    return await Process(s3Event, img, width, quality, isOutWebP);
                }

                var responseContent = string.Empty;

                for (int i = 1; i <= 5; i++)
                {
                    var widthX = response.Metadata["width" + i];
                    if (widthX == null)
                    {
                        break;
                    }

                    var key = await Process(s3Event, img, int.Parse(widthX), quality, isOutWebP);

                    responseContent += key + ";";
                }

                return responseContent.TrimEnd(';');
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        public async Task<string> Process(S3EventNotification.S3Entity s3Event, Image img, int width, int q, bool isWebP)
        {
            await using var memStream = new MemoryStream();

            img.ThumbnailImage(width).JpegsaveStream(memStream);

            var fileTransferUtility = new TransferUtility(S3Client);

            var key = Key.Replace("_raw", "_" + width);

            await fileTransferUtility.UploadAsync(memStream, s3Event.Bucket.Name, key);

            if (isWebP)
            {
                await using var memStream2 = new MemoryStream();

                img.ThumbnailImage(width).WebpsaveStream(memStream2, q);

                fileTransferUtility = new TransferUtility(S3Client);
                var req = new TransferUtilityUploadRequest()
                {
                    ContentType = "image/webp",
                    InputStream = memStream2,
                    BucketName = s3Event.Bucket.Name,
                    Key = key.Replace(Path.GetExtension(Key) ?? "", ".webp")
                };
                await fileTransferUtility.UploadAsync(req);
            }
            return key;
        }
    }
}
