package fr.unice.miage.sd.tinydfs.main;

import java.io.File;
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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fr.unice.miage.sd.tinydfs.nodes.Master;
import fr.unice.miage.sd.tinydfs.nodes.Slave;


public class MasterMain extends UnicastRemoteObject implements Master, Serializable {
    
    private String dfsRootFolder;
    private int nbSlaves;
    private Slave leftSlave;
    private Slave rightSlave;
    private List<byte[]> leftList = new ArrayList<byte[]>();
    private List<byte[]> rightList = new ArrayList<byte[]>();
    
    // Usage: java fr.unice.miage.sd.tinydfs.main.MasterMain storage_service_name dfs_root_folder nb_slaves
    public static void main(String[] args) throws RemoteException,
                AlreadyBoundException, NotBoundException, MalformedURLException, InterruptedException {
            String storageServiceName = args[0];
            String dfsRootFolder = args[1];
            int nbSlaves = Integer.parseInt(args[2]);

            // Create master and register it
            int rmiRegistryPort = 1099;
            Registry registry = LocateRegistry.createRegistry(rmiRegistryPort);
            System.out.println("Master : RMI registry listening on port " + rmiRegistryPort);
            
            MasterMain masterMain = new MasterMain(dfsRootFolder,nbSlaves);
            registry.bind(storageServiceName, masterMain);
            
            // A retirer avant le rendu, executer par le code python
//            String[] param = new String[3];
//            param[0] = "localhost";
//            param[1] = ".";
//            System.out.println("Master : Création des " + nbSlaves + " slaves");
//            for (int i = 0; i < nbSlaves; i++) {
//                param[2] = i + "";
//                SlaveMain.main(param);
//	           }
            
            System.out.println("Master : En attente des slaves");
            boolean slavePret = false;
            while (!slavePret) {
                Thread.sleep(2000);
                slavePret = true;
                for (int i = 2; i < nbSlaves+2; i++) {
                    try {
                        Slave slave = (Slave) Naming.lookup("rmi://localhost/" + "Slave" +i);
                    }
                    catch (NotBoundException ex) {
                        slavePret = false;
                        break;
                    }
                }
            }
            
            System.out.println("Master : Tous les slaves sont créés. Création de l'arbre binaire complet");
            // Verification puis creation de l'arbre binaire complet
            if (nbSlaves > 1 && (Long.bitCount(nbSlaves+2) == 1)) {
                //Slave gauche = (Slave) Naming.lookup("rmi://localhost/" + "Slave2");
                masterMain.setLeftSlave((Slave) Naming.lookup("rmi://localhost/" + "Slave2"));
                masterMain.setRightSlave((Slave) Naming.lookup("rmi://localhost/" + "Slave3"));
                System.out.println("Master : fils gauche et fils droit attribués");
                //System.out.println("Master : fils gauche : " + gauche.getId());
                
                int i = 2;
                while ((2 * i) <= nbSlaves) {
                    Slave filsGauche = (Slave) Naming.lookup("rmi://localhost/" + "Slave" + (2*i));
                    Slave filsDroit = (Slave) Naming.lookup("rmi://localhost/" + "Slave" + ((2*i) + 1));
                    Slave noeud = (Slave) Naming.lookup("rmi://localhost/" + "Slave" + i);
                    noeud.setLeftSlave(filsGauche);
                    noeud.setRightSlave(filsDroit);
                    i++;
                }
                System.out.println("Master : Création de l'arbe binaire complet réussi.");
            }
            else {
                System.out.println("Master : Création de l'arbre binaire complet échoué. Le nombre de Slave ne permet pas de le faire.");
            }
            
            //TESTS
            File file = new File ("C:\\Users\\Dragos\\Workspace\\SysDis\\systeme\\src\\test\\resources\\textual-sample");
	        File file1 = new File ("C:\\Users\\Dragos\\Workspace\\SysDis\\systeme\\src\\test\\resources\\binary-sample");
	        masterMain.saveFile(file);
	        Path path = Paths.get(file1.getAbsolutePath());
	  		try {
	  			byte[] contenu = Files.readAllBytes(path);
	  			masterMain.saveBytes(file1.getName(), contenu);
	  		} catch (IOException e) {
	  			e.printStackTrace();
	  		}
          
	  		masterMain.retrieveFile(file.getName());
           
    }

    public MasterMain(String dfsRootFolder, int nbSlaves) throws RemoteException {
        super();
        this.dfsRootFolder = dfsRootFolder;
        this.nbSlaves = nbSlaves;
    }
    
    private void setLeftSlave(Slave slave) {
        this.leftSlave = slave;
    }
    
    private void setRightSlave(Slave slave) {
        this.rightSlave = slave;
    }
    
    @Override
    public String getDfsRootFolder() throws RemoteException {
        return this.dfsRootFolder;
    }

    @Override
    public int getNbSlaves() throws RemoteException {
        return this.nbSlaves;
    }
    
    @Override
    public void saveFile(File file) throws RemoteException {
    	Path path = Paths.get(file.getAbsolutePath());
		try {
			byte[] contenu = Files.readAllBytes(path);
			saveBytes(file.getName(), contenu);
		} catch (IOException e) {
			e.printStackTrace();
		}

    }

    @Override
    public void saveBytes(String filename, byte[] fileContent) throws RemoteException {
    	/* Principe:
    	 * On crée deux listes de byte[] : une pour le leftSlave et une pour le rightSlave.
    	 * On divise le tableau fileContent en 'nbSlaves' sous-tableaux.
    	 * La moitié des sous-tableaux sera affectée au leftSlave, l'autre moitié au rightSlave.*/

		int start = 0;
		int end;
		
		/* Définir la taille de base d'un sous-tableau 
		 * La méthode Math.ceil permet de récuperer la valeur entière immédiate supérieure à un nb qui n'est pas entier
		 * Cela permet une répartition plus équitable des bytes, si le nombre total de byte n'est pas divisible par le nbSlaves 
		 */		
		int bytesParPart = (int)Math.ceil((float)fileContent.length/nbSlaves);
		
		//Compléter la liste gauche
		while (start < fileContent.length/2) {
			end = Math.min(fileContent.length/2, start + bytesParPart);
			leftList.add(Arrays.copyOfRange(fileContent, start, end));
			start += bytesParPart;
		}
		
		//Compléter la liste droite
		start = fileContent.length/2;
		while (start < fileContent.length) {	
			end = Math.min(fileContent.length, start + bytesParPart);
			rightList.add(Arrays.copyOfRange(fileContent,start, end));
			start += bytesParPart;
		}
    	
		//Affecter les deux listes aux slaves correspondants
    	leftSlave.subSave(filename, leftList);
    	rightSlave.subSave(filename, rightList);
    	
    	System.out.println("Sauvegarde réussie! Congrats");
    }

    @Override
    public File retrieveFile(String filename) throws RemoteException {
	    try {
		    FileOutputStream fileOuputStream = new FileOutputStream(filename, true); 
			fileOuputStream.write(retrieveBytes(filename));
			fileOuputStream.close();
			File resultat = new File (filename);
			return resultat;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return null;
        
    }

    @Override
    public byte[] retrieveBytes(String filename) throws RemoteException {
    	List<byte[]> listeReconst = new ArrayList<byte[]>();
        if (leftSlave != null) {
        	listeReconst.addAll(leftSlave.subRetrieve(filename));
        }
        if (rightSlave != null) {
        	listeReconst.addAll(rightSlave.subRetrieve(filename));
        }
        
    	int totalBytes = 0;
    	for (byte[] byteArray : listeReconst) {
    		for (byte b : byteArray) {
				totalBytes++;
			}
    	}
    	byte[] tab = new byte[totalBytes];
    	for (byte[] byteArray : listeReconst) {
    		for (int i = 0; i < byteArray.length; i++) {
				tab[i] = byteArray[i];
			}
    	}
    	
    	return tab;
    }
}
