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
				
				//CURRENTLY NOT WORKING MANUALLY SUBMIT JOBS INSTEAD
				//TRYING TO SUBMIT EQUIVALENT OF
				//gcloud dataproc jobs submit hadoop 
				//--cluster=cs1660dataproc2 --region=us-central1 
				//--jar=C:\Users\Maxwell\Desktop\Engine.jar 
				//-- gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/in/ gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/out
				
				
				GoogleCredentials credentials = null;
				try {
					credentials = GoogleCredentials.fromStream(new FileInputStream("C:\\Users\\Maxwell\\Desktop\\CS1660project2\\data\\default\\cs1660project2-42bea642ac8d.json"))
					        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
				} catch (FileNotFoundException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
	            Dataproc dataproc=null;
				dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), 
				        new HttpCredentialsAdapter(credentials.createScoped(DataprocScopes.all())))
				        .setApplicationName("SearchEngine").build();	            HadoopJob hJob = new HadoopJob();
	            hJob.setMainJarFileUri("gs://dataproc-staging-us-central1-964478747399-flzelsoc/jars/Engine.jar");
	            hJob.setArgs(ImmutableList.of("gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/in/",
	            		"gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/output/"));
	            
	            try {
					dataproc.projects().regions().jobs().submit("CS1660Project2" , "us-central1", new SubmitJobRequest()
					             .setJob(new Job()
					                .setPlacement(new JobPlacement()
					                    .setClusterName("cs1660dataproc2"))
					             .setHadoopJob(hJob)))
					            .execute();
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
	            
	            //This job fails because of an authentification issue.
				  /*    Job response=null;
					try {
						response = submitJobAsOperationAsyncRequest.get();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (ExecutionException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				*/
				//Download pregenereated results For victor Hugos' Les Mis and Notre Dame.
				ArrayList<byte[]> mergeData = new ArrayList<byte[]>();
		        int arrayLength = 0;

		        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		        
		        Page<Blob> blobs = storage.list(bucket, BlobListOption.prefix("data/out/"));
		        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

		        Iterator<Blob> iterator = blobs.iterateAll().iterator();
		        iterator.next();
		        while (iterator.hasNext()) {
		            Blob blob = iterator.next();
		            blob.downloadTo(byteStream);
		            mergeData.add(byteStream.toByteArray());
		            arrayLength += byteStream.size();
		            byteStream.reset();
		        }
		        try {
					byteStream.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		        byte[] finalMerge = new byte[arrayLength];
		        
		        int destination = 0;
		        for (byte[] data: mergeData) {
		            System.arraycopy(data, 0, finalMerge, destination, data.length);
		            destination += data.length;
		        }

			}
		});

		frame.getContentPane().add(indiciesB);

		frame.setLayout(null);
		frame.setVisible(true);
	}
}
