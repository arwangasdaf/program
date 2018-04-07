import java.util.Scanner;

/**
 * Created by Administrator on 2017/3/23.
 */
public class permutation {
    //全排列函数
    public static void Permutation(String[] list , int k , int m)
    {
        if(k == m)
        {
            for(int i=0 ; i<=m ; i++)
                System.out.print(list[i]);
            System.out.print('\n');
        }else{
            for(int j=k ; j<=m ; j++)
            {
                swap(list[j] , list[k]);
                Permutation(list , k + 1 , m);
                swap(list[j] , list[k]);
            }
        }
    }

    public static void swap(String a  , String b)
    {
        String temp;
        temp = a;
        a = b;
        b = temp;
    }

    public static void main(String[] args)
    {
        Scanner input = new Scanner(System.in);
        String[] list = new String[3];
        for(int i=0 ; i<3 ; i++)
            list[i] = input.next();
        Permutation(list , 0 , 2);
    }
}
