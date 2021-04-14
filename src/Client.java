import javax.swing.*;
import javax.swing.filechooser.*;
import java.awt.*;
import java.awt.event.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.api.gax.paging.Page;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;

public class Client {
	static ArrayList<String> filePaths = new ArrayList<String>();
	static ArrayList<String> fileNames = new ArrayList<String>();

	public static ArrayList<String> stringToList(String s) {
	    return new ArrayList<>(Arrays.asList(s.split(" ")));
	  }
	
	public static void main(String args[]) {
		
		JFrame frame = new JFrame("Client");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(300, 500);
		JButton addfiles = new JButton("Choose files");
		addfiles.setBounds(5, 25, 290, 45);
		
		
		
		addfiles.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				JFileChooser fileChooser = new JFileChooser();
				fileChooser.setCurrentDirectory(new File(System.getProperty("user.dir")));
				fileChooser.setMultiSelectionEnabled(true);
				int result = fileChooser.showOpenDialog(null);
				if (result == JFileChooser.APPROVE_OPTION) {
					File[] selectedFiles = fileChooser.getSelectedFiles();
					for (int i = 0; i < selectedFiles.length; i++) {
						System.out.println("Selected file: " + selectedFiles[i].getAbsolutePath());
						filePaths.add(selectedFiles[i].getAbsolutePath());
						fileNames.add(selectedFiles[i].getName());
					}
				}
			}
		});
		frame.getContentPane().add(addfiles);

		JButton indiciesB = new JButton("Upload files and Construct Inverted Indicies");
		indiciesB.setBounds(5, 85, 290, 105);
		indiciesB.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e){
				System.out.println("uploading");
				// CREATE DATAPROC BUCKET
				String projectId = "CS1660Project2";
				String bucket = "dataproc-staging-us-central1-964478747399-flzelsoc";

				Storage cloudstorage = null;
				try {
					cloudstorage = StorageOptions.newBuilder().setProjectId(projectId)
							.setCredentials(GoogleCredentials.fromStream(new FileInputStream(
									"C:\\Users\\Maxwell\\Desktop\\CS1660project2\\data\\default\\cs1660project2-42bea642ac8d.json")))
									//"C:/Users/Maxwell/Desktop/CS1660project2/data/cs1660project2-0ca809bd911a.json")))
							.build().getService();
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				// UPLOAD FILES TO DATAPROC BUCKET
				for (int i = 0; i < filePaths.size(); i++) {

					try {
						BlobId bID = BlobId.of(bucket, ("data/in/" + fileNames.get(i)));
						BlobInfo blob = BlobInfo.newBuilder(bID).build();
						cloudstorage.create(blob, Files.readAllBytes(Paths.get(filePaths.get(i))));
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				System.out.println("FILES UPLOADED");
				addfiles.setVisible(false);
				indiciesB.setVisible(false);
				
				
				JButton search = new JButton("Search for term");
				search.setBounds(5, 85, 290, 45);
				frame.getContentPane().add(search);
				
				
				JButton topN = new JButton("Top-N");
				topN.setBounds(5, 165, 290, 45);
				frame.getContentPane().add(topN);
				// SUBMIT JOB TO GCP
				// Code based on reference below.
				// https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitHadoopFsJob.java
				
				
				//TRYING TO SUBMIT EQUIVALENT OF
				//gcloud dataproc jobs submit hadoop 
				//--cluster=cs1660dataproc2 --region=us-central1 
				//--jar=C:\Users\Maxwell\Desktop\Engine.jar 
				//-- gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/in/ gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/out
				


				GoogleCredentials credentials=null;
				try {
					credentials = GoogleCredentials.fromStream(new FileInputStream("C:\\Users\\Maxwell\\Desktop\\CS1660project2\\data\\helper\\cs1660project2-a9c34ca5f03e.json"))
		                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));	
					} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
                    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
                    Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),new JacksonFactory(), requestInitializer)
                        .setApplicationName("Engine")
                        .build();
                    Job submittedJob = null;
                    try {
                    	submittedJob = dataproc.projects().regions().jobs().submit(
						    "cs1660project2", "us-central1", new SubmitJobRequest()
						        .setJob(new Job()
						            .setPlacement(new JobPlacement()
						                .setClusterName("cs1660dataproc2"))
						        .setHadoopJob(new HadoopJob()
						            .setMainClass("Engine")
						            .setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-central1-964478747399-flzelsoc/jars/Engine.jar"))
						            .setArgs(ImmutableList.of(
						            		"gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/in/", "gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/output1"
)))))
						.execute();
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
                    //wait for results;
                    try {
						Thread.sleep(180000);
					} catch (InterruptedException e3) {
						// TODO Auto-generated catch block
						e3.printStackTrace();
					}
                    while (true) {
                        try {
                        	cloudstorage = null;
            				try {
            					cloudstorage = StorageOptions.newBuilder().setProjectId(projectId)
            							.setCredentials(GoogleCredentials.fromStream(new FileInputStream(
            									"C:\\Users\\Maxwell\\Desktop\\CS1660project2\\data\\default\\cs1660project2-42bea642ac8d.json")))
            									//"C:/Users/Maxwell/Desktop/CS1660project2/data/cs1660project2-0ca809bd911a.json")))
            							.build().getService();
            				} catch (IOException e2) {
            					// TODO Auto-generated catch block
            					e2.printStackTrace();
            				}
            				StringBuffer output = new StringBuffer();
            				String outputDir = "data/out";
            				
            				Bucket bucket2 = cloudstorage.get("dataproc-staging-us-central1-964478747399-flzelsoc");
                            Page<Blob> blobs = bucket2.list(
                                Storage.BlobListOption.prefix(outputDir));
                            for (Blob blob : blobs.iterateAll()) {
                                String blobContent = new String(blob.getContent());
                                String[] parts = blobContent.split("\t");
                                System.out.println(parts[0]);
                            }
                            System.out.println(output);
                        } catch (Exception e1) {
                            try {
                                Thread.sleep(60000);
                            } catch (InterruptedException e2) {
                                e2.printStackTrace();
                            }
                        }
                    }
			

			}
		});

		frame.getContentPane().add(indiciesB);

		frame.setLayout(null);
		frame.setVisible(true);
	}
}
