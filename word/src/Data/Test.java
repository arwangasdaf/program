package Data;

import java.util.Scanner;

/**
 * Created by Administrator on 2017/5/4.
 */
public class Test {
    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);
        int n = input.nextInt();
        int[][] res = new int[n][3];
        for(int i=0 ; i<n ; i++){
            int ren = input.nextInt();
            for(int j=0 ; j<ren ; j++){
                int a = input.nextInt();
                int b = input.nextInt();
                int c = input.nextInt();
                if(a >= 60){res[i][0]++;}
                if(b >= 60){res[i][1]++;res[i][0]++;}
                if(c >= 60){res[i][2]++;res[i][1]++;res[i][0]++;}
            }
        }
        for(int i=0 ; i<n ; i++){
            System.out.print("Case #"+i+1+":"+" ");
            for(int j=0 ; j<3 ; j++){
                System.out.print(res[i][j] + " ");
            }
            System.out.print('\n');
        }
    }
}
