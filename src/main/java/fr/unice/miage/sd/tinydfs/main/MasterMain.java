package fr.unice.miage.sd.tinydfs.main;

import fr.unice.miage.sd.tinydfs.nodes.Master;
import fr.unice.miage.sd.tinydfs.nodes.Slave;
import java.io.File;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;


public class MasterMain extends UnicastRemoteObject implements Master {
    
    private String dfsRootFolder;
    private int nbSlaves;
    private Slave leftSlave;
    private Slave rightSlave;
    //essai
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
            /*
            // A retirer avant le rendu, executer par le code python
            String[] param = new String[3];
            param[0] = "localhost";
            param[1] = ".";
            System.out.println("Master : Création des " + nbSlaves + " slaves");
            for (int i = 0; i < nbSlaves; i++) {
                param[2] = i + "";
                SlaveMain.main(param);
            }*/
            
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
                while ((2 * i) < nbSlaves) {
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void saveBytes(String filename, byte[] fileContent) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public File retrieveFile(String filename) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] retrieveBytes(String filename) throws RemoteException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
	
}
