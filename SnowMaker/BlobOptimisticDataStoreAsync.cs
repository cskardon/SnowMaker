using System.Collections.Generic;
using System.Net;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Threading.Tasks;

namespace SnowMaker
{
    public class BlobOptimisticDataStoreAsync : IOptimisticDataStoreAsync
    {
        const string SeedValue = "1";

        readonly CloudBlobContainer blobContainer;

        readonly IDictionary<string, Task<ICloudBlob>> blobReferences;
        readonly object blobReferencesLock = new object();

        public BlobOptimisticDataStoreAsync(CloudStorageAccount account, string containerName)
        {
            var blobClient = account.CreateCloudBlobClient();
            blobContainer = blobClient.GetContainerReference(containerName.ToLower());
            blobContainer.CreateIfNotExists();

            blobReferences = new Dictionary<string, Task<ICloudBlob>>();
        }

        public async Task<string> GetDataAsync(string blockName)
        {
            var blobReference = await GetBlobReferenceAsync(blockName);
            using (var stream = new MemoryStream()) {
                await blobReference.DownloadToStreamAsync(stream);
                return Encoding.UTF8.GetString(stream.ToArray());
            }
        }

        public async Task<bool> TryOptimisticWriteAsync(string scopeName, string data)
        {
            var blobReference = await GetBlobReferenceAsync(scopeName);
            try {
                await UploadTextAsync(
                    blobReference,
                    data,
                    AccessCondition.GenerateIfMatchCondition(blobReference.Properties.ETag));
            }
            catch (StorageException exc) {
                if (exc.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    return false;

                throw;
            }
            return true;
        }

        Task<ICloudBlob> GetBlobReferenceAsync(string blockName)
        {
            return blobReferences.GetValue(
                blockName,
                blobReferencesLock,
                () => InitializeBlobReferenceAsync(blockName));
        }

        private async Task<ICloudBlob> InitializeBlobReferenceAsync(string blockName)
        {
            var blobReference = blobContainer.GetBlockBlobReference(blockName);

            if (await blobReference.ExistsAsync())
                return blobReference;

            try {
                await UploadTextAsync(blobReference, SeedValue, AccessCondition.GenerateIfNoneMatchCondition("*"));
            }
            catch (StorageException uploadException) {
                if (uploadException.RequestInformation.HttpStatusCode != (int)HttpStatusCode.Conflict)
                    throw;
            }

            return blobReference;
        }

        static async Task UploadTextAsync(ICloudBlob blob, string text, AccessCondition accessCondition)
        {
            blob.Properties.ContentEncoding = "UTF-8";
            blob.Properties.ContentType = "text/plain";
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(text))) {
                await blob.UploadFromStreamAsync(stream, accessCondition, null, null);
            }
        }
    }
}
