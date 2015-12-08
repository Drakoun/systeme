package fr.unice.miage.sd.tinydfs.main;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import fr.unice.miage.sd.tinydfs.nodes.Slave;


public class SlaveMain extends UnicastRemoteObject implements Slave, Serializable { 
    
    private int id;
    String dfsRootFolder;
    private Slave leftSlave;
    private Slave rightSlave;
    private List<byte[]> leftList = new ArrayList<byte[]>();
    private List<byte[]> rightList = new ArrayList<byte[]>();
    private List<File> fichiers = new ArrayList<File>();
    
    // Usage: java fr.unice.miage.sd.tinydfs.main.SlaveMain master_host dfs_root_folder slave_identifier
    public static void main(String[] args) throws RemoteException,
                NotBoundException, MalformedURLException, AlreadyBoundException {
            String masterHost = args[0];
            String dfsRootFolder = args[1];
            int slaveId = Integer.parseInt(args[2]);

            // Create slave and register it (registration name must be "slave" + slave identifier)
            SlaveMain slaveMain = new SlaveMain(slaveId, dfsRootFolder);
            System.out.println("Slave" + slaveMain.getId() + " : Créé");
            Registry registry = (Registry) Naming.lookup("rmi://" + masterHost);
            registry.bind("Slave" + slaveMain.getId(), slaveMain);
            System.out.println("Slave" + slaveMain.getId() + " : Enregistré dans le RMI registry");
            
    }

    public SlaveMain(int id, String dfsRootFolder) throws RemoteException {
        super();
        this.id = id + 2;
        this.dfsRootFolder = dfsRootFolder;
    }
    
    @Override
    public int getId() throws RemoteException {
        return this.id;
    }

    @Override
    public Slave getLeftSlave() throws RemoteException {
        return this.leftSlave;
    }

    @Override
    public Slave getRightSlave() throws RemoteException {
        return this.rightSlave;
    }

    @Override
    public void setLeftSlave(Slave slave) throws RemoteException {
        this.leftSlave = slave;
    }

    @Override
    public void setRightSlave(Slave slave) throws RemoteException {
        this.rightSlave = slave;
    }
    
    @Override
    public void subSave(String filename, List<byte[]> subFileContent) throws RemoteException {
    	/* Chaque Slave aura deux "tâches" à accomplir une fois qu'il aura reçu la liste du Master
    	 * ou de son Slave père :
    	 * 		#1	D'abord, il va enregistrer le premier élément de la liste.*/
    	System.out.println("slave " + this.id + " subFileContent : " + subFileContent );
    	File fichier = new File(dfsRootFolder,filename + this.id);
    	fichiers.add(fichier);
		try {
			
			FileOutputStream fos = new FileOutputStream(fichier.getAbsolutePath());
			fos.write(subFileContent.get(0));
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		/* 		#2	Ensuite, il va diviser le reste des éléments en deux sous-listes et va les passer
		 * 		à ses Slaves fils.*/
        
		//List<byte[]> leftList = subFileContent.subList(1, subFileContent.size()/2 + 1);
		System.out.println("slave " + this.id + " la moitié du size " +subFileContent.size()+ " du subFileContent est " + (int) Math.ceil((double)subFileContent.size()/2));

		for (int i = 1; i < (int) Math.ceil((double)subFileContent.size()/2); i++) {
			leftList.add(subFileContent.get(i));
		}
		
        //List<byte[]> rightList = subFileContent.subList(subFileContent.size()/2 + 1, subFileContent.size());
		for (int i = (int) (Math.ceil((double)subFileContent.size()/2)); i < subFileContent.size(); i++) {
			rightList.add(subFileContent.get(i));
		}
        
		System.out.println("slave " + this.id + " et " + this.leftSlave);
		if (leftSlave != null) {
        	leftSlave.subSave(filename, leftList);
        }
		System.out.println("slave " + this.id + " et " + this.rightSlave);
        if (rightSlave != null) {
        	System.out.println("slave " + this.id + " je suis dans la boucle droite");
        	rightSlave.subSave(filename, rightList);
        }
        

    }

    @Override
    public List<byte[]> subRetrieve(String filename) throws RemoteException {
    	List<byte[]> listeReconst = new ArrayList<byte[]>();
    	
    	Path path = Paths.get(getFile(fichiers, filename + this.id).getAbsolutePath());
    	//System.out.println(path);
    	try {
			byte[] contenu = Files.readAllBytes(path);
			listeReconst.add(contenu);
		} catch (IOException e) {
			e.printStackTrace();
		}
		

        if (leftSlave != null) {
        	listeReconst.addAll(leftSlave.subRetrieve(filename));
        }
        if (rightSlave != null) {
        	listeReconst.addAll(rightSlave.subRetrieve(filename));
        }
        return listeReconst;
    }
    
	public static File getFile(List<File> fichiers, String filename) {
		File fichier;
		for (File f : fichiers) {
			if (f.getName().equals(filename)) {
				fichier = new File(filename);
				return fichier;
			}
		}
		return null;
	}
	

}