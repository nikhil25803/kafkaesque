package consumers

import "testing"

func TestGroupStateCategory(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{raw: "Stable", want: GroupStateStable},
		{raw: " stable ", want: GroupStateStable},
		{raw: "PreparingRebalance", want: GroupStateRebalancing},
		{raw: "CompletingRebalance", want: GroupStateRebalancing},
		{raw: "Assigning", want: GroupStateRebalancing},
		{raw: "Reconciling", want: GroupStateRebalancing},
		{raw: "Empty", want: GroupStateEmpty},
		{raw: "Dead", want: GroupStateDead},
		{raw: "Unknown", want: GroupStateUnknown},
		{raw: "", want: GroupStateUnknown},
		{raw: "FutureState", want: GroupStateUnknown},
	}

	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			got := GroupStateCategory(test.raw)
			if got != test.want {
				t.Fatalf("GroupStateCategory(%q) = %q, want %q", test.raw, got, test.want)
			}
			if !IsGroupStateCategory(got) {
				t.Fatalf("normalized state %q is not a valid filter", got)
			}
		})
	}
}

func TestIsGroupStateCategoryRejectsRawAndInvalidStates(t *testing.T) {
	for _, state := range []string{"", "PreparingRebalance", "FutureState"} {
		if IsGroupStateCategory(state) {
			t.Fatalf("IsGroupStateCategory(%q) = true", state)
		}
	}
}
