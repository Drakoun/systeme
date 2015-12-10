package fr.unice.miage.sd.tinydfs.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    	 * 		#1	D'abord, il va enregistrer le premier élément de la liste.
    	 */
    	
    	File fichier = new File(dfsRootFolder,filename + this.id);
		try {
			
			FileOutputStream fos = new FileOutputStream(fichier.getAbsolutePath());
			fos.write(subFileContent.get(0));
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
                
                fichiers.add(fichier);
                System.out.println("J'ai sauvegardé " + fichier + " sur mon disque.");
        
		List<byte[]> leftList = new ArrayList<byte[]>();
        List<byte[]> rightList = new ArrayList<byte[]>();
		/* 		#2	Ensuite, il va diviser le reste des éléments en deux sous-listes et va les passer
		 * 		à ses Slaves fils.
		 */
        
		for (int i = 1; i < (int) Math.ceil((double)subFileContent.size()/2); i++) {
			leftList.add(subFileContent.get(i));
		}
		
		for (int i = (int) (Math.ceil((double)subFileContent.size()/2)); i < subFileContent.size(); i++) {
			rightList.add(subFileContent.get(i));
		}
        
		if (leftSlave != null) {
        	leftSlave.subSave(filename, leftList);
        }

        if (rightSlave != null) {
        	rightSlave.subSave(filename, rightList);
        }
        

    }

    @Override
    public List<byte[]> subRetrieve(String filename) throws RemoteException {
    	List<byte[]> listeReconst = new ArrayList<byte[]>();
    	/* Chaque slave devra reconstituer la liste de tableaux de byte à partir des sous-listes des slaves fils gauche et droit.
    	 * 		#1	D'abord, il rajoute le contenu de son propre fichier correspondant à la requête.
    	 * 		(ce fichier se trouve dans une liste de fichiers qu'il detient, avec tous les fichiers (parties) qu'il
    	 * 		a gardés sur son disque)
    	 */
    	Path path = Paths.get(getFile(fichiers, filename + this.id).toString());
    	try {
			byte[] contenu = Files.readAllBytes(path);
			listeReconst.add(contenu);
		} catch (IOException e) {
			e.printStackTrace();
		}
		

    	/*
    	 * 		#2	Ensuite, il demande à ses fils gauche et droit de compléter la liste à reconstituer.
    	 */
        if (leftSlave != null) {
        	listeReconst.addAll(leftSlave.subRetrieve(filename));
        }
        if (rightSlave != null) {
        	listeReconst.addAll(rightSlave.subRetrieve(filename));
        }
        return listeReconst;
    }
    
    @Override
    public long sizeOf(String filename) throws RemoteException {
            long taille = getFile(fichiers, filename + this.id).length();
            if (leftSlave != null) {
                    taille += leftSlave.sizeOf(filename);
            }
            if (rightSlave != null) {
                    taille += rightSlave.sizeOf(filename);
            }
            return taille;
    }
    
    //Méthode qui retourne un fichier spécifique d'une liste de fichiers.
    public File getFile(List<File> fichiers, String filename) {
            File fichier;
            for (File f : fichiers) {
                    if (f.toString().endsWith(filename)) {
                            fichier = new File(f.toString());
                            return fichier;
                    }
            }
            return null;
    }
}