import java.awt.event.*;  
import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Storage.BlobListOption;


public class Client {
	static ArrayList<String> filePaths = new ArrayList<String>();
static ArrayList<String> fileNames = new ArrayList<String>();

public static void main(String args[]){
JFrame frame = new JFrame("Client");
frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
frame.setSize(300,500);
JButton addfiles = new JButton("Choose files");
addfiles.setBounds(5,25,295,45);



addfiles.addActionListener(new ActionListener(){
	public void actionPerformed(ActionEvent e){
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setCurrentDirectory(new File(System.getProperty("user.dir")));
		fileChooser.setMultiSelectionEnabled(true);
		int result = fileChooser.showOpenDialog(null);
		if (result == JFileChooser.APPROVE_OPTION) {
		    File[] selectedFiles = fileChooser.getSelectedFiles();
		    for(int i=0;i<selectedFiles.length;i++){
		        System.out.println("Selected file: " + selectedFiles[i].getAbsolutePath());
						filePaths.add(selectedFiles[i].getAbsolutePath());
						fileNames.add(selectedFiles[i].getName());
					}
		        }
		}
	});
	frame.getContentPane().add(addfiles);
	
	JButton indiciesB = new JButton("Construct Inverted Indicies");
	indiciesB.setBounds(5,85,295,105);
	indiciesB.addActionListener(new ActionListener(){
		public void actionPerformed(ActionEvent e){
			System.out.println("uploading");
			//Upload files to Google Cloud Bucket
			String projectid = "CS1660Project2";
			String bucket = "dataproc-staging-us-central1-964478747399-flzelsoc";
	
			Storage storage=null;
			try {
				storage = StorageOptions.newBuilder().setProjectId(projectid).setCredentials(GoogleCredentials.fromStream(new 
				         FileInputStream("C:/Users/Maxwell/Desktop/CS1660project2/data/cs1660project2-515cf45dadaf.json"))).build().getService();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			for(int i=0;i<filePaths.size();i++){
				
				try {
					BlobId bID = BlobId.of(bucket, ("data/"+fileNames.get(i)));
					BlobInfo blob = BlobInfo.newBuilder(bID).build();
					storage.create(blob,Files.readAllBytes(Paths.get(filePaths.get(i))));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			
		}
	});
		
		
        frame.getContentPane().add(indiciesB);
        
		frame.setLayout(null);
		frame.setVisible(true);
	}
}

