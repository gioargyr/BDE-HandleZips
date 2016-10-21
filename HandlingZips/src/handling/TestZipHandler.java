package handling;

import java.io.IOException;

public class TestZipHandler {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		ZipHandler3 zipHandler = new ZipHandler3();
		String outcome = zipHandler.tiffToHDFS(args[0], "natha");
		System.out.println(outcome);

	}

}
