import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import segy.SegyImage;

class DistributedFile {
  
  public DistributedFile ( String segy_file
                         , String header
                         , int o1 , int o2 , int o3
                         , int n1 , int n2 , int n3
                         , int d1 , int d2 , int d3
                         , int p1 , int p2 , int p3
                         , String prefix
                         , String suffix
                         , String dir
                         ) throws VolumeIOException
  {
      _header = header;
      generate_header ( _header
                      , o1 , o2 , o3
                      , n1 , n2 , n3
                      , d1 , d2 , d3
                      , p1 , p2 , p3
                      , prefix
                      , suffix
                      , dir
                      );
      init_dim();
      init(_header);
      check_errors();
      generate_directories();
      generate_filenames();
      clean();
      populate_from_segy(segy_file);
  }
  
  private void populate_from_segy(String segy_file) throws VolumeIOException
  {
      SegyImage image = new SegyImage(segy_file);
      int NX = ((int)((image.getN3()-1)/p1)+1)*p1;
      int NY = ((int)((image.getN2()-1)/p2)+1)*p2;
      int NZ = ((int)((image.getN1()-1)/p3)+1)*p3;
      float[][][] data = new float[NX][NY][NZ];
      for(int i3=image.getI3Min();i3<=image.getI3Max();i3++)
      {
          for(int i2=image.getI2Min();i2<=image.getI2Max();i2++)
          {
              float[] trace = image.getTrace(i2, i3);
              float[] out = data[i3-image.getI3Min()][i2-image.getI2Min()];
              for(int i1=0;i1<image.getN1();i1++)
              {
                  out[i1] = trace[i1];
              }
          }
      }
      write_data_local_coordinates(0, 0, 0, data);
  }
  
  public DistributedFile ( float[][][] data
                         , String header
                         , int o1 , int o2 , int o3
                         , int n1 , int n2 , int n3
                         , int d1 , int d2 , int d3
                         , int p1 , int p2 , int p3
                         , String prefix
                         , String suffix
                         , String dir
                         ) throws VolumeIOException
  {
      _header = header;
      generate_header ( _header
                      , o1 , o2 , o3
                      , n1 , n2 , n3
                      , d1 , d2 , d3
                      , p1 , p2 , p3
                      , prefix
                      , suffix
                      , dir
                      );
      init_dim();
      init(_header);
      check_errors();
      generate_directories();
      generate_filenames();
      clean();
      populate_from_array(data);
  }
  
  private void populate_from_array(float[][][] data) throws VolumeIOException
  {
      if(data.length!=n1)
      {
          throw VolumeIOException.fromMessage("populate from array : n1 mismatch");
      }
      if(data[0].length!=n2)
      {
          throw VolumeIOException.fromMessage("populate from array : n2 mismatch");
      }
      if(data[0][0].length!=n3)
      {
          throw VolumeIOException.fromMessage("populate from array : n3 mismatch");
      }
      if(n1%p1!=0)
      {
          throw VolumeIOException.fromMessage("populate from array : n1 should be evenly divisible by p1");
      }
      if(n2%p2!=0)
      {
          throw VolumeIOException.fromMessage("populate from array : n2 should be evenly divisible by p2");
      }
      if(n3%p3!=0)
      {
          throw VolumeIOException.fromMessage("populate from array : n3 should be evenly divisible by p3");
      }
      write_data_local_coordinates(0, 0, 0, data);
  }
  
  public DistributedFile ( String header
                         , int o1 , int o2 , int o3
                         , int n1 , int n2 , int n3
                         , int d1 , int d2 , int d3
                         , int p1 , int p2 , int p3
                         , String prefix
                         , String suffix
                         , String dir
                         ) throws VolumeIOException
  {
      _header = header;
      generate_header ( _header
                      , o1 , o2 , o3
                      , n1 , n2 , n3
                      , d1 , d2 , d3
                      , p1 , p2 , p3
                      , prefix
                      , suffix
                      , dir
                      );
      init_dim();
      init(_header);
      check_errors();
      generate_directories();
      generate_filenames();
      clean();
  }
  
  private void generate_header  ( String header
                                , int o1 , int o2 , int o3
                                , int n1 , int n2 , int n3
                                , int d1 , int d2 , int d3
                                , int p1 , int p2 , int p3
                                , String prefix
                                , String suffix
                                , String dir
                                ) throws VolumeIOException
  {
      try
      {
          PrintWriter file = new PrintWriter(header);
          
          file.println("o1="+o1);
          file.println("o2="+o2);
          file.println("o3="+o3);
          
          file.println("n1="+n1);
          file.println("n2="+n2);
          file.println("n3="+n3);
          
          file.println("d1="+d1);
          file.println("d2="+d2);
          file.println("d3="+d3);
          
          file.println("p1="+p1);
          file.println("p2="+p2);
          file.println("p3="+p3);
          
          file.println("prefix="+prefix);
          file.println("suffix="+suffix);
          file.println("dir="+dir);
          
          file.close();
      }
      catch(IOException e)
      {
          throw VolumeIOException.fromMessage(e.getMessage());
      }
  }
  
  public DistributedFile(String header) throws VolumeIOException
  {
    _header = header;
    init_dim();
    init(_header);
    check_errors();
    generate_directories();
    generate_filenames();
  }
    
  private void parse(String line) throws VolumeIOException
  {
    if(line.length()>0)
    {
      if(line.getBytes()[0] == '#')
      {
        //System.out.println("comment:"+line);
        // ignore
        return;
      }
      if(line.substring(0,3).contains("o1="))
      {
        o1 = Integer.parseInt(line.substring(3));
        //System.out.println("o1="+o1);
        return;
      }
      if(line.substring(0,3).contains("o2="))
      {
        o2 = Integer.parseInt(line.substring(3));
        //System.out.println("o2="+o2);
        return;
      }
      if(line.substring(0,3).contains("o3="))
      {
        o3 = Integer.parseInt(line.substring(3));
        //System.out.println("o3="+o3);
        return;
      }
      if(line.substring(0,3).contains("n1="))
      {
        n1 = Integer.parseInt(line.substring(3));
        p1 = n1;
        //System.out.println("n1="+n1);
        return;
      }
      if(line.substring(0,3).contains("n2="))
      {
        n2 = Integer.parseInt(line.substring(3));
        p2 = n2;
        //System.out.println("n2="+n2);
        return;
      }
      if(line.substring(0,3).contains("n3="))
      {
        n3 = Integer.parseInt(line.substring(3));
        p3 = n3;
        //System.out.println("n3="+n3);
        return;
      }
      if(line.substring(0,3).contains("d1="))
      {
        d1 = Integer.parseInt(line.substring(3));
        //System.out.println("d1="+d1);
        return;
      }
      if(line.substring(0,3).contains("d2="))
      {
        d2 = Integer.parseInt(line.substring(3));
        //System.out.println("d2="+d2);
        return;
      }
      if(line.substring(0,3).contains("d3="))
      {
        d3 = Integer.parseInt(line.substring(3));
        //System.out.println("d3="+d3);
        return;
      }
      if(line.substring(0,3).contains("p1="))
      {
        p1 = Integer.parseInt(line.substring(3));
        //System.out.println("p1="+p1);
        return;
      }
      if(line.substring(0,3).contains("p2="))
      {
        p2 = Integer.parseInt(line.substring(3));
        //System.out.println("p2="+p2);
        return;
      }
      if(line.substring(0,3).contains("p3="))
      {
        p3 = Integer.parseInt(line.substring(3));
        //System.out.println("p3="+p3);
        return;
      }
      if(line.substring(0,7).contains("prefix="))
      {
        prefix = line.substring(7);
        //System.out.println("prefix="+prefix);
        return;
      }
      if(line.substring(0,7).contains("suffix="))
      {
        suffix = line.substring(7);
        //System.out.println("suffix="+suffix);
        return;
      }
      if(line.substring(0,4).contains("dir="))
      {
        String dir = line.substring(4);
        dirs.add(new Dir(dir));
        //System.out.println("dir="+dir);
        return;
      }
      //System.out.println("dont understand:"+line);
    }
  }

  private void init(String header) throws VolumeIOException
  {
      try
      {
          File file = new File(header);
          FileReader fileReader = new FileReader(file);
          BufferedReader bufferedReader = new BufferedReader(fileReader);
          StringBuffer stringBuffer = new StringBuffer();
          String line;
          while ((line = bufferedReader.readLine()) != null) {
            parse(line);
            stringBuffer.append(line);
            stringBuffer.append("\n");
          }
          fileReader.close();
      }
      catch(IOException e)
      {
          throw VolumeIOException.fromMessage(e.getMessage());
      }
  }
  
  private void init_dim()
  {
    o1 = -1;
    o2 = -1;
    o3 = -1;
    
    n1 = -1;
    n2 = -1;
    n3 = -1;
    
    d1 = -1;
    d2 = -1;
    d3 = -1;
    
    p1 = -1;
    p2 = -1;
    p3 = -1;
    
    prefix = "";
    
    suffix = ".bin";
    
    dirs = new ArrayList<Dir>();
  }
  
  private void check_errors() throws VolumeIOException
  {
    if(o1 < 0)
    {
      throw VolumeIOException.fromMessage("o1 < 0");
    }
    if(o2 < 0)
    {
      throw VolumeIOException.fromMessage("o2 < 0");
    }
    if(o3 < 0)
    {
      throw VolumeIOException.fromMessage("o3 < 0");
    }
    
    if(n1 < 1)
    {
      throw VolumeIOException.fromMessage("n1 < 1");
    }
    if(n2 < 1)
    {
      throw VolumeIOException.fromMessage("n2 < 1");
    }
    if(n3 < 1)
    {
      throw VolumeIOException.fromMessage("n3 < 1");
    }
    
    if(p1 < 1)
    {
      throw VolumeIOException.fromMessage("p1 < 1");
    }
    if(p2 < 1)
    {
      throw VolumeIOException.fromMessage("p2 < 1");
    }
    if(p3 < 1)
    {
      throw VolumeIOException.fromMessage("p3 < 1");
    }
    
    if(prefix=="")
    {
      throw VolumeIOException.fromMessage("prefix empty");
    }
    
    
  }
  
  private class Dir
  {
    List<String> data;
    String get_dir(int index)
    {
      return data.get(1+index);
    }
    int size()
    {
      return data.size()-1;
    }
    Dir(String dir) throws VolumeIOException
    {
      int quote_count=0;
      int start_ind = 0;
      int end_ind = 0;
      String line;
      data = new ArrayList<String>();
      for(int index=0;index<dir.length();index++)
      {
        if(dir.getBytes()[index]=='"')
        {
          quote_count++;
        }
        if(quote_count%2==0)
        {
          if(dir.getBytes()[index]==' '||index+1==dir.length())
          {
            end_ind = index;
            line = dir.substring(start_ind, end_ind);
            start_ind = index+1;
            data.add(line);
          }
        }
      }
      
      if(data.size()<2)
      {
        throw VolumeIOException.fromMessage("malformed dir:"+dir);
      }
      
      //System.out.println("ip:"+data.get(0));
      
      for(int i=1;i<data.size();i++)
      {
        data.set(i, data.get(i).replaceAll("\"", ""));
        //System.out.println("dir:"+data.get(i));
      }
      
    }
  }
  
  private void generate_directories()
  {
    directories = new ArrayList<String>();
    for(int k=0;k<dirs.size();k++)
    {
      for(int i=0;i<dirs.get(k).size();i++)
      {
        directories.add(dirs.get(k).get_dir(i));
      }
    }
    //System.out.println("directories:"+directories.size());
  }
  
  private void generate_filenames() throws VolumeIOException
  {
    int f1 = (int)Math.ceil((float)n1/(float)p1);
    int f2 = (int)Math.ceil((float)n2/(float)p2);
    int f3 = (int)Math.ceil((float)n3/(float)p3);
    //System.out.println("f1:"+f1+" f2:"+f2+" f3:"+f3);
    filenames = new String[f1][f2][f3];
    mutex = new ReentrantLock[f1][f2][f3];
    for(int i1=0,k=0;i1<f1;i1++)
    {
      for(int i2=0;i2<f2;i2++)
      {
        for(int i3=0;i3<f3;i3++,k++)
        {
          filenames[i1][i2][i3] = directories.get(k%directories.size())+"/"+prefix+(i1*p1)+"_"+(i2*p2)+"_"+(i3*p3)+suffix;
          mutex[i1][i2][i3] = new ReentrantLock(true);
          //System.out.println(filenames[i1][i2][i3]);
        }
      }
    }
  }
  
  public void clean() throws VolumeIOException
  {
    for(int i1=0,k=0;i1<filenames.length;i1++)
    {
      for(int i2=0;i2<filenames[0].length;i2++)
      {
        for(int i3=0;i3<filenames[0][0].length;i3++,k++)
        {
          filenames[i1][i2][i3] = directories.get(k%directories.size())+"/"+prefix+(i1*p1)+"_"+(i2*p2)+"_"+(i3*p3)+suffix;
          //System.out.println("deleting:"+filenames[i1][i2][i3]);
          File f = new File(filenames[i1][i2][i3]);
          if(f.exists())
          {
            boolean bool = f.delete();
            if(bool)
            {
              //System.out.println("file deleted:"+filenames[i1][i2][i3]);
            }
          }
          else
          {
            //throw new IOException("file to be deleted "+filenames[i1][i2][i3]+" does not exist.");
          }
        }
      }
    }
  }
  
  public void purge() throws VolumeIOException
  {
    for(int i=0;i<directories.size();i++)
    {
      File folder = new File(directories.get(i)+"/");
      File[] listOfFiles = folder.listFiles();
      for(File f : listOfFiles)
      {
        //System.out.println("deleting:"+f.getAbsolutePath());
        if(f.exists()&&f.isFile())
        {
          boolean bool = f.delete();
          if(bool)
          {
            //System.out.println("file deleted:"+f.getAbsolutePath());
          }
        }
        else
        {
          //throw new IOException("file to be deleted "+filenames[i1][i2][i3]+" does not exist.");
        }
      }
    }
  }

  public float read_data_global_coords(int i1,int i2,int i3) throws VolumeIOException
  {
    if(i1<o1)
    {
      throw VolumeIOException.fromMessage("i1 < o1");
    }
    if(i1>o1+d1*n1)
    {
      throw VolumeIOException.fromMessage("i1 > o1+d1*n1");
    }
    if(i2<o2)
    {
      throw VolumeIOException.fromMessage("i2 < o2");
    }
    if(i2>o2+d2*n2)
    {
      throw VolumeIOException.fromMessage("i2 > o2+d2*n2");
    }
    if(i3<o3)
    {
      throw VolumeIOException.fromMessage("i3 < o3");
    }
    if(i3>o3+d3*n3)
    {
      throw VolumeIOException.fromMessage("i3 > o3+d3*n3");
    }
    if((i1-o1)%d1!=0)
    {
      throw VolumeIOException.fromMessage("(i1-o1) mod d1 != 0");
    }
    if((i2-o2)%d2!=0)
    {
      throw VolumeIOException.fromMessage("(i2-o2) mod d2 != 0");
    }
    if((i3-o3)%d3!=0)
    {
      throw VolumeIOException.fromMessage("(i3-o3) mod d3 != 0");
    }
    try
    {
      FileInputStream in = new FileInputStream(filenames[(i1-o1)/(p1*d1)][(i2-o2)/(p2*d2)][(i3-o3)/(p3*d3)]);
      int orig1 = ((i1-o1)/(p1*d1))*p1;
      int orig2 = ((i2-o2)/(p2*d2))*p2;
      int orig3 = ((i3-o3)/(p3*d3))*p3;
      int skip = 4*((((i1-o1)/d1-orig1)*p2+((i2-o2)/d2-orig2))*p3+((i3-o3)/d3-orig3));
      long ret = in.skip(skip);
      if(ret>=0)
      {
        ByteBuffer buf = ByteBuffer.allocate(4);
        in.read(buf.array());
        in.close();
        return buf.getFloat(0);
      }
      else
      {
        in.close();
        throw VolumeIOException.fromMessage("index outside of range:"+i1+" ["+o1+","+(o1+n1)+"]; "+i2+" ["+o2+","+(o2+n2)+"]; "+i3+" ["+o3+","+(o3+n3)+"]; ");
      }
    }
    catch(IOException e)
    {
        throw VolumeIOException.fromMessage(e.getMessage());
    }
  }
  
  public float read_data_local_coords(int i1,int i2,int i3) throws VolumeIOException
  {
    if(i1<0)
    {
      throw VolumeIOException.fromMessage("i1 < 0");
    }
    if(i1>n1)
    {
      throw VolumeIOException.fromMessage("i1 > n1");
    }
    if(i2<0)
    {
      throw VolumeIOException.fromMessage("i2 < 0");
    }
    if(i2>n2)
    {
      throw VolumeIOException.fromMessage("i2 > n2");
    }
    if(i3<0)
    {
      throw VolumeIOException.fromMessage("i3 < 0");
    }
    if(i3>n3)
    {
      throw VolumeIOException.fromMessage("i3 > n3");
    }
    try
    {
      FileInputStream in = new FileInputStream(filenames[i1/p1][i2/p2][i3/p3]);
      int orig1 = (i1/p1)*p1;
      int orig2 = (i2/p2)*p2;
      int orig3 = (i3/p3)*p3;
      int skip = 4*(((i1-orig1)*p2+(i2-orig2))*p3+(i3-orig3));
      long ret = in.skip(skip);
      if(ret>=0)
      {
        ByteBuffer buf = ByteBuffer.allocate(4);
        in.read(buf.array());
        in.close();
        return buf.getFloat(0);
      }
      else
      {
        in.close();
        throw VolumeIOException.fromMessage("index outside of range:"+i1+" ["+o1+","+(o1+n1)+"]; "+i2+" ["+o2+","+(o2+n2)+"]; "+i3+" ["+o3+","+(o3+n3)+"]; ");
      }
    }
    catch(IOException e)
    {
        throw VolumeIOException.fromMessage(e.getMessage());
    }
  }
  
  public float[][][] read_data_local_coords(int or1,int or2,int or3,int nr1,int nr2,int nr3) throws VolumeIOException
  {
    if(or1<0)
    {
      throw VolumeIOException.fromMessage("or1 < 0");
    }
    if(or1+nr1>n1)
    {
      throw VolumeIOException.fromMessage("or1+nr1 > n1");
    }
    if(or2<0)
    {
      throw VolumeIOException.fromMessage("or2 < 0");
    }
    if(or2+nr2>n2)
    {
      throw VolumeIOException.fromMessage("or2+nr2 > n2");
    }
    if(or3<0)
    {
      throw VolumeIOException.fromMessage("or3 < 0");
    }
    if(or3+nr3>n3)
    {
      throw VolumeIOException.fromMessage("or3+nr3 > n3");
    }
  
    int min1=n1,max1=0;
    int min2=n2,max2=0;
    int min3=n3,max3=0;
    global_mutex.lock();
    workers = new ArrayList<Worker>();
    for(int i=0;i<directories.size()*Runtime.getRuntime().availableProcessors();i++)
    {
      workers.add(new TileReadWorker(i));
    }

    int index = 0;
    for (int i1 = 0; i1 < filenames.length; i1++) {
      for (int i2 = 0; i2 < filenames[0].length; i2++) {
        for (int i3 = 0; i3 < filenames[0][0].length; i3++) {
          if (i1 * p1 >= (or1 / p1) * p1 && i1 * p1 < ((or1 + nr1) / p1) * p1 + p1) {
            if (i2 * p2 >= (or2 / p2) * p2 && i2 * p2 < ((or2 + nr2) / p2) * p2 + p2) {
              if (i3 * p3 >= (or3 / p3) * p3 && i3 * p3 < ((or3 + nr3) / p3) * p3 + p3) {
                Tile tile = new Tile(i1 * p1, i2 * p2, i3 * p3, filenames[i1][i2][i3],
                    mutex[i1][i2][i3]);
                workers.get(index % workers.size()).add(tile);
                index++;
                min1 = Math.min(i1 * p1, min1);
                min2 = Math.min(i2 * p2, min2);
                min3 = Math.min(i3 * p3, min3);
                max1 = Math.max(i1 * p1 + p1, max1);
                max2 = Math.max(i2 * p2 + p2, max2);
                max3 = Math.max(i3 * p3 + p3, max3);
              }
            }
          }
        }

      }

    }
    //System.out.println(min1+" "+max1);
    //System.out.println(min2+" "+max2);
    //System.out.println(min3+" "+max3);
    if(max1<min1||max2<min2||max3<min3)
    {
      global_mutex.unlock();
      throw VolumeIOException.fromMessage("probe outside of volume.");
    }
    tmp_buf = new float[max1-min1][max2-min2][max3-min3];
    O1 = min1;
    O2 = min2;
    O3 = min3;
    
    runAll(workers);
    
    joinAll(workers);
    
    workers = null;

    output_buf = new float[nr1][nr2][nr3];
    for(int x1=or1;x1<or1+nr1;x1++)
    {
      for(int x2=or2;x2<or2+nr2;x2++)
      {
        for(int x3=or3;x3<or3+nr3;x3++)
        {
          output_buf[x1-or1][x2-or2][x3-or3] = tmp_buf[x1-min1][x2-min2][x3-min3]; 
        }
      }
    }
    
    tmp_buf = null;
    global_mutex.unlock();
    return output_buf;
  }
  
  
  public float[][][] read_data()
  {
    global_mutex.lock();
    output_buf = new float[n1][n2][n3];
    workers = new ArrayList<Worker>();
    for(int i=0;i<directories.size()*Runtime.getRuntime().availableProcessors();i++)
    {
      workers.add(new TotalReadWorker(i));
    }
    
    int index = 0;
    for(int i1=0;i1<filenames.length;i1++)
    {
      for(int i2=0;i2<filenames[0].length;i2++)
      {
        for(int i3=0;i3<filenames[0][0].length;i3++,index++)
        {
          Tile tile = new Tile(i1*p1,i2*p2,i3*p3,filenames[i1][i2][i3],mutex[i1][i2][i3]);
          workers.get(index%workers.size()).add(tile);
        }
      }
    }

    runAll(workers);
    
    joinAll(workers);
    
    workers = null;

    global_mutex.unlock();
    return output_buf;
  }
  

  public void write_data_local_coordinates(int or1,int or2,int or3,final float[][][] D) throws VolumeIOException
  {
    int nr1 = D.length;
    int nr2 = D[0].length;
    int nr3 = D[0][0].length;
        
    if(or1<0)
    {
      throw VolumeIOException.fromMessage("or1 < 0");
    }
    if(or1+nr1>n1)
    {
      throw VolumeIOException.fromMessage("or1+nr1 > n1");
    }
    if(or2<0)
    {
      throw VolumeIOException.fromMessage("or2 < 0");
    }
    if(or2+nr2>n2)
    {
      throw VolumeIOException.fromMessage("or2+nr2 > n2");
    }
    if(or3<0)
    {
      throw VolumeIOException.fromMessage("or3 < 0");
    }
    if(or3+nr3>n3)
    {
      throw VolumeIOException.fromMessage("or3+nr3 > n3");
    }
    
    global_mutex.lock();
    
    tmp_buf = new float[nr1][nr2][nr3];
    for(int x1=0;x1<nr1;x1++)
    {
      for(int x2=0;x2<nr2;x2++)
      {
        for(int x3=0;x3<nr3;x3++)
        {
          tmp_buf[x1][x2][x3] = D[x1][x2][x3];
        }
      }
    }
    
    workers = new ArrayList<Worker>();
    for(int i=0;i<directories.size()*Runtime.getRuntime().availableProcessors();i++)
    {
      workers.add(new TileWriterWorker(i));
    }
    
    int min1=n1,max1=0;
    int min2=n2,max2=0;
    int min3=n3,max3=0;
    int index = 0;
    
    for (int i1 = 0; i1 < filenames.length; i1++) {
      for (int i2 = 0; i2 < filenames[0].length; i2++) {
        for (int i3 = 0; i3 < filenames[0][0].length; i3++) {
          if (i1 * p1 >= (or1 / p1) * p1 && i1 * p1 < ((or1 + nr1) / p1) * p1 + p1) {
            if (i2 * p2 >= (or2 / p2) * p2 && i2 * p2 < ((or2 + nr2) / p2) * p2 + p2) {
              if (i3 * p3 >= (or3 / p3) * p3 && i3 * p3 < ((or3 + nr3) / p3) * p3 + p3) {
                Tile tile = new Tile(i1 * p1, i2 * p2, i3 * p3, filenames[i1][i2][i3],
                    mutex[i1][i2][i3]);
                tile.or1 = or1;
                tile.or2 = or2;
                tile.or3 = or3;
                tile.nr1 = nr1;
                tile.nr2 = nr2;
                tile.nr3 = nr3;
                workers.get(index % workers.size()).add(tile);
                index++;
                min1 = Math.min(i1 * p1, min1);
                min2 = Math.min(i2 * p2, min2);
                min3 = Math.min(i3 * p3, min3);
                max1 = Math.max(i1 * p1 + p1, max1);
                max2 = Math.max(i2 * p2 + p2, max2);
                max3 = Math.max(i3 * p3 + p3, max3);
              }
            }
          }
        }
      }
    }
    
    runAll(workers);
    
    joinAll(workers);
    
    workers = null;
    
    tmp_buf = null;
    
    global_mutex.unlock();
    
  }
  
  public void write_data(final float[][][] D,boolean remove_existing) throws VolumeIOException
  {
    if(D.length!=n1)
    {
      throw VolumeIOException.fromMessage("n1 mismatch");
    }
    if(D[0].length!=n2)
    {
      throw VolumeIOException.fromMessage("n2 mismatch");
    }
    if(D[0][0].length!=n3)
    {
      throw VolumeIOException.fromMessage("n3 mismatch");
    }
    global_mutex.lock();
    if(remove_existing)
    {
      clean();
    }
    
    tmp_buf = new float[n1][n2][n3];
    for(int x1=0;x1<n1;x1++)
    {
      for(int x2=0;x2<n2;x2++)
      {
        for(int x3=0;x3<n3;x3++)
        {
          tmp_buf[x1][x2][x3] = D[x1][x2][x3];
        }
      }
    }
    
    workers = new ArrayList<Worker>();
    for(int i=0;i<directories.size()*Runtime.getRuntime().availableProcessors();i++)
    {
      workers.add(new TotalWriterWorker(i));
    }
    
    int index = 0;
    for(int i1=0;i1<filenames.length;i1++)
    {
      for(int i2=0;i2<filenames[0].length;i2++)
      {
        for(int i3=0;i3<filenames[0][0].length;i3++,index++)
        {
          Tile tile = new Tile(i1*p1,i2*p2,i3*p3,filenames[i1][i2][i3],mutex[i1][i2][i3]);
          workers.get(index%workers.size()).add(tile);
        }
      }
    }
    
    runAll(workers);
    
    joinAll(workers);
    
    workers = null;
    
    tmp_buf = null;
    
    global_mutex.unlock();
    
  } 
  
  private class Tile
  {
    int or1,or2,or3;
    int nr1,nr2,nr3;
    int o1,o2,o3;
    String filename;
    Lock mutex;
    Tile(int o1,int o2,int o3,String filename,Lock mutex)
    {
      this.o1 = o1;
      this.o2 = o2;
      this.o3 = o3;
      this.filename = filename;
      this.mutex = mutex;
    }
    void lock()
    {
      mutex.lock();
    }
    void unlock()
    {
      mutex.unlock();
    }
  }
  
  public abstract class Worker implements Runnable
  {
    
    protected List<Tile> tiles;
      
      private Thread thread;
      
      public Worker (int index)
      {
        this.tiles = new ArrayList<Tile>();
        thread = new Thread(this, String.format("Worker-%d", index));
      }

      public void add(Tile tile)
      {
        tiles.add(tile);
      }
      
      public void start()
      {
        thread.start();
      }
      
      public boolean join(int ms) throws Throwable
      {
        thread.join(ms);
        return !thread.isAlive();
      }


      
  }


  public class TotalReadWorker extends DistributedFile.Worker
  {
    
    float [] arr;
    
    ByteBuffer rbuf;
    
    ByteBuffer buf;

    public TotalReadWorker(int index) {
      super(index);
      
      buf = ByteBuffer.allocate(Float.BYTES*p1*p2*p3);
    }

    @Override
    public void run()
      {
      for(int k=0;k<tiles.size();k++)
      {
        try {
          Tile tile = tiles.get(k);
          tile.lock();
          FileInputStream in = new FileInputStream(tile.filename);
          in.read(buf.array());
          in.close();
          for(int x1=tile.o1,i=0;x1<tile.o1+p1;x1++)
          {
            for(int x2=tile.o2;x2<tile.o2+p2;x2++)
            {
              for(int x3=tile.o3;x3<tile.o3+p3;x3++,i++)
              {
                output_buf[x1][x2][x3] = buf.getFloat(4*i); 
              }
            }
          }
          tile.unlock();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          tiles.get(k).unlock();
          e.printStackTrace();
        }
      }
      tiles.clear();
      }
  }
  
  public class TileReadWorker extends DistributedFile.Worker
  {
    
    float [] arr;
    
    ByteBuffer rbuf;
    
    ByteBuffer buf;

    public TileReadWorker(int index) {
      super(index);
      
      buf = ByteBuffer.allocate(Float.BYTES*p1*p2*p3);
    }

    @Override
    public void run()
      {
      for(int k=0;k<tiles.size();k++)
      {
        try {
          Tile tile = tiles.get(k);
          tile.lock();
          FileInputStream in = new FileInputStream(tile.filename);
          in.read(buf.array());
          in.close();
          float max=0;
          for(int x1=tile.o1-O1,i=0;x1<tile.o1-O1+p1;x1++)
          {
            for(int x2=tile.o2-O2;x2<tile.o2-O2+p2;x2++)
            {
              for(int x3=tile.o3-O3;x3<tile.o3-O3+p3;x3++,i++)
              {
                tmp_buf[x1][x2][x3] = buf.getFloat(4*i); 
                max = Math.max(tmp_buf[x1][x2][x3], max);
              }
            }
          }
          //System.out.println(tile.filename+":"+max);
          tile.unlock();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          tiles.get(k).unlock();
          e.printStackTrace();
        }
      }
      tiles.clear();
      }
  }

  public class TotalWriterWorker extends DistributedFile.Worker
  {
    
    float [] arr;
    
    ByteBuffer rbuf;
    
    ByteBuffer buf;

    public TotalWriterWorker(int index) {
      super(index);

      arr = new float[p1*p2*p3];
      
      buf = ByteBuffer.allocate(Float.BYTES*arr.length);
    }

    @Override
    public void run()
      {
      for(int k=0;k<tiles.size();k++)
      {
        try {
          Tile tile = tiles.get(k);
          tile.lock();
          for(int x1=tile.o1,i=0;x1<tile.o1+p1;x1++)
          {
            for(int x2=tile.o2;x2<tile.o2+p2;x2++)
            {
              for(int x3=tile.o3;x3<tile.o3+p3;x3++,i++)
              {
                arr[i] = tmp_buf[x1][x2][x3]; 
              }
            }
          }
          buf.asFloatBuffer().put(arr);
          FileOutputStream out = new FileOutputStream(tile.filename);
          out.write(buf.array());
          out.close();
          tile.unlock();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          tiles.get(k).unlock();
          e.printStackTrace();
        }
      }
      tiles.clear();
      }
  }
  
  public class TileWriterWorker extends DistributedFile.Worker
  {
    
    float [] arr;
    
    ByteBuffer rbuf;
    
    ByteBuffer buf;

    public TileWriterWorker(int index) {
      super(index);

      arr = new float[p1*p2*p3];
      
      rbuf = ByteBuffer.allocate(Float.BYTES*arr.length);
      
      buf = ByteBuffer.allocate(Float.BYTES*arr.length);
    }

    @Override
    public void run()
      {
      for(int k=0;k<tiles.size();k++)
      {
        try 
        {
          Tile tile = tiles.get(k);
          tile.lock();
          
          File f = new File(tile.filename);
          if(f.exists())
          {
            FileInputStream in = new FileInputStream(tile.filename);
            in.read(rbuf.array());
            in.close();
            for(int x1=tile.o1,i=0;x1<tile.o1+p1;x1++)
            {
              for(int x2=tile.o2;x2<tile.o2+p2;x2++)
              {
                for(int x3=tile.o3;x3<tile.o3+p3;x3++,i++)
                {
                  if(x1-tile.or1>=0&&x1-tile.or1<tile.nr1&&x2-tile.or2>=0&&x2-tile.or2<tile.nr2&&x3-tile.or3>=0&&x3-tile.or3<tile.nr3)
                  {
                    arr[i] = tmp_buf[x1-tile.or1][x2-tile.or2][x3-tile.or3];
                  }
                  else
                  {
                    arr[i] = rbuf.getFloat(4*i);
                  }
                }
              }
            }
          }
          else
          {
            for(int x1=tile.o1,i=0;x1<tile.o1+p1;x1++)
            {
              for(int x2=tile.o2;x2<tile.o2+p2;x2++)
              {
                for(int x3=tile.o3;x3<tile.o3+p3;x3++,i++)
                {
                  if ( x1-tile.or1>=0 && x1-tile.or1<tile.nr1
                    && x2-tile.or2>=0 && x2-tile.or2<tile.nr2
                    && x3-tile.or3>=0 && x3-tile.or3<tile.nr3
                     )
                  {
                    arr[i] = tmp_buf[x1-tile.or1][x2-tile.or2][x3-tile.or3];
                  }
                }
              }
            }
          }
          
          buf.asFloatBuffer().put(arr);
          FileOutputStream out = new FileOutputStream(tile.filename);
          out.write(buf.array());
          out.close();
          
          tile.unlock();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          tiles.get(k).unlock();
          e.printStackTrace();
        }
      }
      tiles.clear();
      }
  }
  
  void runAll(List<Worker> workers)
  {
    for(Worker worker:workers)if(worker != null)worker.start();
  }
  
  void joinAll(List<Worker> workers)
  {
      try{
        for(Worker worker:workers)if(worker != null)((DistributedFile.Worker) worker).join(0);
      }
      catch (Throwable e) {
        e.printStackTrace();
      }
  }
  
  public int get_n1(){return n1;}
  public int get_n2(){return n2;}
  public int get_n3(){return n3;}
  
  private List<Worker> workers;
  
  private String _header;
  
  private int o1;
  private int o2;
  private int o3;
  
  private int n1;
  private int n2;
  private int n3;
  
  private int d1;
  private int d2;
  private int d3;
  
  private int p1;
  private int p2;
  private int p3;
  
  private String prefix;
  
  private String suffix;
  
  private List<Dir> dirs;
  
  private List<String> directories;
  
  private String[][][] filenames;
  
  private Lock[][][] mutex;
  
  private float[][][] output_buf;
  
  private float[][][] tmp_buf;
  
  private int O1,O2,O3;
  
  private Lock global_mutex = new ReentrantLock(true); //protects the above, in case the user wants to use this class in parallel
  
  
}
