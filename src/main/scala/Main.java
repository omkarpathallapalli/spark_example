import java.util.*;

public class Main {
	public static void main(String[] args) {
		int a[] = { 2, 1, 3, 5, 3, 2 };
		Set<Integer> Items = new HashSet<>();
		System.out.println(Arrays.stream(a).filter(i -> !Items.add(i)).findFirst().orElse(-1));
	}
}
