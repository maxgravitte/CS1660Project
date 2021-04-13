import java.awt.event.*;
import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.dataproc.v1.*;

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
		addfiles.setBounds(5, 25, 295, 45);
		
		
		
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

		JButton indiciesB = new JButton("Construct Inverted Indicies");
		indiciesB.setBounds(5, 85, 295, 105);
		indiciesB.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				System.out.println("uploading");
				// CREATE DATAPROC BUCKET
				String projectId = "CS1660Project2";
				String bucket = "dataproc-staging-us-central1-964478747399-flzelsoc";

				Storage cloudstorage = null;
				try {
					cloudstorage = StorageOptions.newBuilder().setProjectId(projectId)
							.setCredentials(GoogleCredentials.fromStream(new FileInputStream(
									"C:/Users/Maxwell/Desktop/CS1660project2/data/cs1660project2-0ca809bd911a.json")))
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
				// SUBMIT JOB TO GCP
				// Code based on reference below.
				// https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitHadoopFsJob.java

				String region = "us-central1";
				String clusterName = "cs1660dataproc";
				String hadoopFspath = "gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/in"
						+ " " + "gs://dataproc-staging-us-central1-964478747399-flzelsoc/data/out";

				String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

				JobControllerSettings jobControllerSettings = null;
				// Configure the settings for the job controller client.
				try {
					jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				System.out.println("Submitting job");
				try (JobControllerClient jobControllerClient = JobControllerClient.create(jobControllerSettings)) {

					// Configure cluster placement for the job.
					JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();

					// Configure Hadoop job settings. The HadoopFS query is set here.
					HadoopJob hadoopJob = HadoopJob.newBuilder().setMainClass("org.apache.hadoop.fs.FsShell")
							.addAllArgs(stringToList(hadoopFspath)).build();

					Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();

					// Submit an asynchronous request to execute the job.
					OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest = jobControllerClient
							.submitJobAsOperationAsync(projectId, region, job);

					Job response=null;
					try {
						response = submitJobAsOperationAsyncRequest.get();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

					// Print output from Google Cloud Storage.
					Matcher matches = Pattern.compile("gs://(.*?)/(.*)").matcher(response.getDriverOutputResourceUri());
					matches.matches();

					Storage storage = StorageOptions.getDefaultInstance().getService();
					Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));

					System.out.println(String.format("Job finished successfully: %s", new String(blob.getContent())));

				} catch (ExecutionException e2) {
					// If the job does not complete successfully, print the error message.
					System.err.println(String.format("submitHadoopFSJob: %s ", e2.getMessage()));
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
			}
		});

		frame.getContentPane().add(indiciesB);

		frame.setLayout(null);
		frame.setVisible(true);
	}
}
