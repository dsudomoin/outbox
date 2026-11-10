package prodcheck.store;

import org.springframework.data.jpa.repository.JpaRepository;

/** Spring Data's repository of the business rows. */
interface StoreOrders extends JpaRepository<StoreOrder, Long> {}
