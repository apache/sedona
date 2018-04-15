package org.datasyslab.geosparkviz.utils;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public class AzureBlobOperator {

    private CloudStorageAccount storageAccount;
    private CloudBlobClient blobClient;
    private CloudBlobContainer container;

    private String storageConnectionString;
    private String containerName;

    public final static Logger logger = Logger.getLogger(AzureBlobOperator.class);

    public AzureBlobOperator(String containerName, String accountName, String accountKey) {
        String accountNameSeg = "AccountName=" + accountName + ";";
        String accountKeySeg = "AccountKey=" + accountKey + ";";
        storageConnectionString = "DefaultEndpointsProtocol=https;" + accountNameSeg + accountKeySeg;
        this.containerName = containerName;
    }

    public void init() throws URISyntaxException, InvalidKeyException, StorageException {
        storageAccount = CloudStorageAccount.parse(storageConnectionString);
        blobClient = storageAccount.createCloudBlobClient();
        container = blobClient.getContainerReference(containerName);

        logger.info("[GeoSparkViz][SaveRasterImageAsAzureBlob] Creating container: " + containerName);
        container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());

    }

    public boolean putImage(String containerName, String path, BufferedImage rasterImage)
            throws URISyntaxException, StorageException {
        deleteImage(containerName, path);
        ByteArrayOutputStream outputStream = null;
        InputStream inputStream = null;

        try {
            outputStream = new ByteArrayOutputStream();
            ImageIO.write(rasterImage, "png", outputStream);
            byte[] buffer = outputStream.toByteArray();
            inputStream = new ByteArrayInputStream(buffer);
            CloudBlockBlob blockBlob = container.getBlockBlobReference(path);
            blockBlob.upload(inputStream, -1);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public boolean deleteImage(String containerName, String path)
            throws URISyntaxException, StorageException {
        CloudBlockBlob blockBlob = container.getBlockBlobReference(path);
        blockBlob.deleteIfExists();
        logger.info("[GeoSparkViz][deleteImage] Deleted an image if exist");
        return true;
    }

    public BufferedImage getImage(String path)
            throws URISyntaxException, StorageException, IOException {
        logger.debug("[GeoSparkViz][getImage] Start");
        CloudBlockBlob blob = container.getBlockBlobReference(path);
        InputStream inputStream = blob.openInputStream();
        BufferedImage rasterImage = ImageIO.read(inputStream);
        inputStream.close();
        logger.info("[GeoSparkViz][getImage] Got an image");
        return rasterImage;
    }

}
