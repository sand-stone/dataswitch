package kdb.presto;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class Main {

  public static void main(String[] args) throws Exception {
    FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(new FileInputStream(args[0]));
    Descriptor numd = null;
    for (FileDescriptorProto fdp : descriptorSet.getFileList()) {
      FileDescriptor fd = FileDescriptor.buildFrom(fdp, new FileDescriptor[] {});
      for (Descriptor desc : fd.getMessageTypes()) {
        System.out.println("xxxdesc:" + desc.getName());
        if(desc.getName().equals("Number"))
          numd = desc;
        for(FieldDescriptor f : desc.getFields()) {
          System.out.println("field:" + f);
        }
        //FieldDescriptor fdesc = desc.findFieldByName("protoType");
      }
    }
    byte[] data = Files.readAllBytes(Paths.get(args[1]));
    DynamicMessage msg = DynamicMessage.parseFrom(numd, data);
    //System.out.println("msg:" + msg);
    System.out.println("type:" + numd.findFieldByName("value").getType());
    System.out.println("value:" + msg.getField(numd.findFieldByName("value")));
  }

}
