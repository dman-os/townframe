
module task_ticket_algebra {
  import task_ticket as ticket from "./task_ticket"
  import task_ticket_checks as checks from "./task_ticket_checks"
  pure val samples = powerset(checks::inputs).map(checks::oracle)
  var a: ticket::Ticket
  var b: ticket::Ticket
  var c: ticket::Ticket
  action init = { nondet x = samples.oneOf(); nondet y = samples.oneOf(); nondet z = samples.oneOf()
    all { a' = x, b' = y, c' = z } }
  action step = all { a' = a, b' = b, c' = c }
  val algebra = ticket::merge(a,a).ticket == a
    and ticket::merge(a,b).error == 0
    and ticket::merge(a,b).ticket == ticket::merge(b,a).ticket
    and ticket::merge(ticket::merge(a,b).ticket,c).ticket == ticket::merge(a,ticket::merge(b,c).ticket).ticket
}
